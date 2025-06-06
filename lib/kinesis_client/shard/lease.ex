defmodule KinesisClient.Stream.Shard.Lease do
  @moduledoc """
  Monitors lease ownership for a specific shard and manages the shard processor lifecycle.

  """
  require Logger
  use GenServer
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.AppState.ShardLease
  alias KinesisClient.Stream.Shard.Pipeline
  alias KinesisClient.Stream.Shard

  # How often to check for lease ownership changes
  @default_check_interval 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(opts[:shard_id]))
  end

  defstruct [
    :app_name,
    :shard_id,
    :lease_owner,
    :worker_id,
    :lease_count,
    :app_state_opts,
    :check_interval,
    :notify,
    :lease_holder,
    :timer
  ]

  @type t :: %__MODULE__{}

  @impl GenServer
  def init(opts) do
    # Apply jitter to check interval to prevent thundering herd
    base_interval = Keyword.get(opts, :check_interval, @default_check_interval)
    jitter_factor = :rand.uniform() * 0.4 + 0.8
    check_interval = trunc(base_interval * jitter_factor)

    worker_id = opts[:lease_owner]

    state = %__MODULE__{
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      worker_id: worker_id,
      # We'll set this based on the current lease
      lease_owner: nil,
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      check_interval: check_interval,
      notify: Keyword.get(opts, :notify),
      lease_holder: false,
      lease_count: 0,
      timer: nil
    }

    Logger.debug("Starting KinesisClient.Stream.Lease monitor: #{inspect(state)}")
    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    # Check the current state of the lease
    new_state = check_lease_ownership(state)

    # Schedule periodic checks for lease ownership changes
    timer = schedule_lease_check(state.check_interval)
    new_state = %{new_state | timer: timer}

    notify({:initialized, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:check_lease, state) do
    # Stop the timer before checking
    if state.timer do
      Process.cancel_timer(state.timer)
    end

    # Get the current lease status
    lease = AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts)

    case lease do
      # If the lease is nil or has a different owner
      nil ->
        Logger.warning("Lease for shard #{state.shard_id} not found, stopping")
        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      # If the lease status is "STOPPING" - this is the signal from the leader to stop processing
      %{lease_status: "STOPPING"} ->
        Logger.warning("Lease for shard #{state.shard_id} marked as STOPPING, stopping processor",
          ansi_color: :yellow
        )

        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      # If we no longer own the lease
      %{lease_owner: lease_owner} when lease_owner != state.worker_id ->
        Logger.warning(
          "Lost lease for shard #{state.shard_id}, current owner: #{lease_owner}, stopping"
        )

        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      # If the lease is completed (by the processor), we don't need to renew
      %{completed: true} ->
        Logger.info("Lease for shard #{state.shard_id} is completed, stopping")
        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      # We own the lease, and it's not completed
      _ ->
        if not state.lease_holder do
          Logger.info("Gained ownership of lease for shard #{state.shard_id}")
          send(state.notify, {:lease_acquired, state.shard_id})
        end

        # Schedule the next check
        new_timer = Process.send_after(self(), :check_lease, state.check_interval)
        {:noreply, %{state | lease_holder: true, timer: new_timer}}
    end
  end

  @impl GenServer
  def handle_info(unexpected_message, state) do
    Logger.warning("Lease process received unexpected message: #{inspect(unexpected_message)}")
    {:noreply, state}
  end

  # Helper to stop the pipeline and terminate
  defp stop_pipeline_and_terminate(state) do
    # Notify that we're stopping - use the notify helper that handles nil case
    notify({:lease_lost, state.shard_id}, state)

    # Check if we've already tried to stop this pipeline and shard
    pipeline_name = Pipeline.name(state.app_name, state.shard_id)
    stopping_pipelines = Process.get(:stopping_pipelines, MapSet.new())
    stopping_shards = Process.get(:stopping_shards, MapSet.new())

    # Mark the shard as being stopped to prevent duplicate attempts
    Process.put(:stopping_shards, MapSet.put(stopping_shards, state.shard_id))

    # Only attempt to stop the pipeline if we haven't tried already
    if !MapSet.member?(stopping_pipelines, pipeline_name) do
      # Mark the pipeline as being stopped
      Process.put(:stopping_pipelines, MapSet.put(stopping_pipelines, pipeline_name))

      # Check if the pipeline is actually running before trying to stop it
      pipeline_process = Process.whereis(pipeline_name)

      if is_pid(pipeline_process) do
        # Try to stop the pipeline
        try do
          Pipeline.stop(state.app_name, state.shard_id)
        catch
          :exit, {:normal, _} ->
            # This is expected and not an error
            Logger.info("Broadway pipeline for #{state.shard_id} exited normally")
            :ok

          _kind, error ->
            Logger.error("Error stopping pipeline: #{inspect(error)}")
        end
      else
        Logger.info("Pipeline for #{state.shard_id} is not running, no need to stop it")
      end
    else
      Logger.info(
        "Pipeline for #{state.shard_id} is already being stopped, skipping redundant stop"
      )
    end
  end

  # Check if this worker is the current lease holder and respond to changes
  defp check_lease_ownership(state) do
    case get_lease(state) do
      %ShardLease{completed: true} ->
        # If shard is completed, closing shard
        Logger.info("Shard #{state.shard_id} is completed, closing shard")
        # Coordinator.close_shard(state.shard_id)
        %{state | lease_holder: false}

      %ShardLease{} = lease ->
        # Check if lease ownership has changed
        current_owner = lease.lease_owner
        ownership_changed = current_owner != state.lease_owner
        is_owner = current_owner == state.worker_id

        # Update state with latest lease info
        updated_state = %{
          state
          | lease_owner: current_owner,
            lease_count: lease.lease_count,
            lease_holder: is_owner
        }

        cond do
          # We gained ownership of the lease - start the pipeline
          ownership_changed && is_owner ->
            Logger.info("Gained ownership of lease for shard #{state.shard_id}",
              ansi_color: :green_background
            )

            :ok = Pipeline.start(state.app_name, state.shard_id)
            notify({:lease_acquired, updated_state}, state)

            updated_state

          # We lost ownership of the lease - stop both pipeline and shard process
          ownership_changed && !is_owner && state.lease_holder ->
            Logger.info("Lost ownership of lease for shard #{state.shard_id} to #{current_owner}",
              ansi_color: :yellow_background
            )

            # Track both pipelines and shards that we've already attempted to stop
            pipeline_name = Pipeline.name(state.app_name, state.shard_id)
            stopping_pipelines = Process.get(:stopping_pipelines, MapSet.new())
            stopping_shards = Process.get(:stopping_shards, MapSet.new())

            # Mark the shard as being stopped
            Process.put(:stopping_shards, MapSet.put(stopping_shards, state.shard_id))

            # Only try to stop the pipeline if it's running and we haven't tried already
            pipeline_process = Process.whereis(pipeline_name)

            if is_pid(pipeline_process) && !MapSet.member?(stopping_pipelines, pipeline_name) do
              # Mark the pipeline as being stopped
              Process.put(:stopping_pipelines, MapSet.put(stopping_pipelines, pipeline_name))

              # Stop the pipeline first
              try do
                Pipeline.stop(state.app_name, state.shard_id)
              catch
                :exit, {:normal, _} ->
                  # This is expected and not an error
                  Logger.info("Broadway pipeline for #{state.shard_id} exited normally")
                  :ok

                _kind, error ->
                  Logger.error("Error stopping pipeline: #{inspect(error)}")
              end
            else
              if is_pid(pipeline_process) do
                Logger.info(
                  "Pipeline for #{state.shard_id} is already being stopped, skipping redundant stop"
                )
              else
                Logger.info("Pipeline for #{state.shard_id} is not running")
              end
            end

            # Also stop the shard process
            shard_name = Shard.name(state.app_name, state.shard_id)
            Logger.info("Stopping shard process: #{inspect(shard_name)}")
            # Coordinator.close_shard(state.shard_id)

            notify({:lease_lost, updated_state}, state)
            updated_state

          # No ownership change, update state
          true ->
            if ownership_changed do
              notify({:lease_owner_changed, updated_state}, state)
            end

            updated_state
        end

      :not_found ->
        # Lease doesn't exist yet, just monitor
        Logger.debug("No lease found for shard #{state.shard_id}, continuing to monitor")
        state

      {:error, e} ->
        Logger.error("Error fetching lease for shard #{state.shard_id}: #{inspect(e)}")
        state
    end
  end

  defp get_lease(state) do
    AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts)
  end

  defp schedule_lease_check(interval) do
    Process.send_after(self(), :check_lease, interval)
  end

  defp notify(_msg, %{notify: nil}) do
    :ok
  end

  defp notify(msg, %{notify: notify}) do
    send(notify, msg)
    :ok
  end

  defp name(shard_id) do
    Module.concat(__MODULE__, shard_id)
  end
end
