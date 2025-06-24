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
    config = Keyword.fetch!(opts, :config)
    base_interval = Keyword.get(opts, :check_interval, config[:lease_check_interval_ms])
    jitter_factor = :rand.uniform() * 0.4 + 0.8
    check_interval = trunc(base_interval * jitter_factor)

    worker_id = opts[:lease_owner]

    state = %__MODULE__{
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      worker_id: worker_id,
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
    new_state = check_lease_ownership(state)

    timer = schedule_lease_check(state.check_interval)
    new_state = %{new_state | timer: timer}

    notify({:initialized, new_state}, state)

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:check_lease, state) do
    if state.timer do
      Process.cancel_timer(state.timer)
    end

    lease = AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts)

    case lease do
      nil ->
        Logger.warning("Lease for shard #{state.shard_id} not found, stopping")
        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      %{lease_status: "STOPPING"} ->
        Logger.warning("Lease for shard #{state.shard_id} marked as STOPPING, stopping processor",
          ansi_color: :yellow
        )

        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      %{lease_owner: lease_owner} when lease_owner != state.worker_id ->
        Logger.warning(
          "Lost lease for shard #{state.shard_id}, current owner: #{lease_owner}, stopping"
        )

        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      %{completed: true} ->
        Logger.info("Lease for shard #{state.shard_id} is completed, stopping")
        stop_pipeline_and_terminate(state)
        {:stop, :normal, %{state | lease_holder: false, timer: nil}}

      _ ->
        if not state.lease_holder do
          Logger.info("Gained ownership of lease for shard #{state.shard_id}")
          send(state.notify, {:lease_acquired, state.shard_id})
        end

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
    notify({:lease_lost, state.shard_id}, state)

    pipeline_name = Pipeline.name(state.app_name, state.shard_id)
    stopping_pipelines = Process.get(:stopping_pipelines, MapSet.new())
    stopping_shards = Process.get(:stopping_shards, MapSet.new())

    Process.put(:stopping_shards, MapSet.put(stopping_shards, state.shard_id))

    if !MapSet.member?(stopping_pipelines, pipeline_name) do
      Process.put(:stopping_pipelines, MapSet.put(stopping_pipelines, pipeline_name))

      pipeline_process = Process.whereis(pipeline_name)

      if is_pid(pipeline_process) do
        try do
          Pipeline.stop(state.app_name, state.shard_id)
        catch
          :exit, {:normal, _} ->
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
        Logger.info("Shard #{state.shard_id} is completed, closing shard")
        %{state | lease_holder: false}

      %ShardLease{} = lease ->
        current_owner = lease.lease_owner
        ownership_changed = current_owner != state.lease_owner
        is_owner = current_owner == state.worker_id

        updated_state = %{
          state
          | lease_owner: current_owner,
            lease_count: lease.lease_count,
            lease_holder: is_owner
        }

        cond do
          ownership_changed && is_owner ->
            Logger.info("Gained ownership of lease for shard #{state.shard_id}",
              ansi_color: :green_background
            )

            :ok = Pipeline.start(state.app_name, state.shard_id)
            notify({:lease_acquired, updated_state}, state)

            updated_state

          ownership_changed && !is_owner && state.lease_holder ->
            Logger.info("Lost ownership of lease for shard #{state.shard_id} to #{current_owner}",
              ansi_color: :yellow_background
            )

            pipeline_name = Pipeline.name(state.app_name, state.shard_id)
            stopping_pipelines = Process.get(:stopping_pipelines, MapSet.new())
            stopping_shards = Process.get(:stopping_shards, MapSet.new())

            Process.put(:stopping_shards, MapSet.put(stopping_shards, state.shard_id))

            pipeline_process = Process.whereis(pipeline_name)

            if is_pid(pipeline_process) && !MapSet.member?(stopping_pipelines, pipeline_name) do
              Process.put(:stopping_pipelines, MapSet.put(stopping_pipelines, pipeline_name))

              try do
                Pipeline.stop(state.app_name, state.shard_id)
              catch
                :exit, {:normal, _} ->
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

            shard_name = Shard.name(state.app_name, state.shard_id)
            Logger.info("Stopping shard process: #{inspect(shard_name)}")

            notify({:lease_lost, updated_state}, state)
            updated_state

          true ->
            if ownership_changed do
              notify({:lease_owner_changed, updated_state}, state)
            end

            updated_state
        end

      :not_found ->
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
