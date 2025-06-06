defmodule KinesisClient.Stream.ShardManager do
  @moduledoc """
  Manages the lifecycle of shard processing tasks within KCL.

  The ShardManager is responsible for:
  1. Starting and stopping shard processes
  2. Monitoring shard processes for failures
  3. Maintaining the registry of active shard processes
  4. Periodically polling for lease assignments

  This component follows KCL 3.x architecture by relying on lease table polling
  rather than direct messages, making it compatible with Java KCL's approach.
  """
  use GenServer
  require Logger
  alias KinesisClient.Stream.Shard
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.AppState.ShardLease

  # Default polling interval for lease changes (5 seconds)
  @default_poll_interval_ms 5_000

  # Client API

  @doc """
  Starts the ShardManager process.

  ## Options
    * `:app_name` - Required. The name of the application/stream.
    * `:stream_name` - Required. The Kinesis stream name.
    * `:shard_supervisor_name` - Required. Name of the DynamicSupervisor for shards.
    * `:worker_id` - Required. The ID of the current worker.
    * `:shard_args` - Required. Arguments to pass to new shard processes.
    * `:app_state_opts` - Optional. Options for the app state store.
    * `:poll_interval_ms` - Optional. Interval in milliseconds for polling lease changes.
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    GenServer.start_link(__MODULE__, opts, name: name(app_name))
  end

  @doc """
  Mark a shard as completed and clean up associated resources.
  """
  def close_shard(app_name, shard_id) do
    GenServer.cast(name(app_name), {:close_shard, shard_id})
  end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    stream_name = Keyword.fetch!(opts, :stream_name)
    shard_supervisor_name = Keyword.fetch!(opts, :shard_supervisor_name)
    worker_id = Keyword.fetch!(opts, :worker_id)
    shard_args = Keyword.fetch!(opts, :shard_args)
    app_state_opts = Keyword.get(opts, :app_state_opts, [])
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)

    Logger.info("Starting ShardManager for #{app_name}")

    state = %{
      app_name: app_name,
      stream_name: stream_name,
      shard_supervisor_name: shard_supervisor_name,
      worker_id: worker_id,
      shard_args: shard_args,
      app_state_opts: app_state_opts,
      poll_interval_ms: poll_interval_ms,
      poll_timer: nil,
      # Map of %{monitor_ref => shard_id}
      shard_ref_map: %{},
      # Track the leases we know about
      current_leases: %{}
    }

    # Schedule first lease check
    poll_timer = schedule_lease_check(poll_interval_ms)

    {:ok, %{state | poll_timer: poll_timer}}
  end

  @impl GenServer
  def handle_cast({:close_shard, shard_id}, state) do
    # Mark the shard as closed in the app state
    case AppState.get_lease(state.app_name, shard_id, state.app_state_opts) do
      %{lease_owner: lease_owner} = lease when not is_nil(lease_owner) ->
        Logger.info("Marking shard #{shard_id} as completed")
        AppState.close_shard(state.app_name, shard_id, lease_owner, state.app_state_opts)

        # Stop the shard process
        stop_shard(state.stream_name, shard_id)

        # Update our internal state
        {ref, cleaned_map} = remove_shard_reference(shard_id, state.shard_ref_map)
        if ref, do: Process.demonitor(ref, [:flush])

        # Update the current_leases to reflect the completion
        current_leases =
          Map.update(state.current_leases, shard_id, lease, fn l ->
            %{l | completed: true}
          end)

        {:noreply, %{state | shard_ref_map: cleaned_map, current_leases: current_leases}}

      _ ->
        Logger.warning("Cannot close shard #{shard_id}: no lease found or no owner")
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:check_lease_changes, state) do
    # Execute the lease check and schedule the next one
    {:noreply, check_for_lease_changes(state)}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # A monitored shard process has terminated
    case Map.fetch(state.shard_ref_map, ref) do
      {:ok, shard_id} ->
        Logger.warning("Shard process for #{shard_id} terminated: #{inspect(reason)}")

        # Clean up the reference
        new_shard_ref_map = Map.delete(state.shard_ref_map, ref)

        # Check if we still own the lease before restarting
        new_state =
          case AppState.get_lease(state.app_name, shard_id, state.app_state_opts) do
            %{lease_owner: owner} when owner == state.worker_id ->
              # We still own the lease, restart the shard
              Logger.info("Still own lease for shard #{shard_id}, restarting process")

              restart_shard(
                shard_id,
                %{state | shard_ref_map: new_shard_ref_map},
                new_shard_ref_map
              )

            lease ->
              # Update current_leases with the latest info
              current_leases =
                if lease != :not_found,
                  do: Map.put(state.current_leases, shard_id, lease),
                  else: state.current_leases

              Logger.info("No longer own lease for shard #{shard_id}, not restarting")
              %{state | shard_ref_map: new_shard_ref_map, current_leases: current_leases}
          end

        {:noreply, new_state}

      :error ->
        # Unknown reference, ignore
        {:noreply, state}
    end
  end

  # Private functions

  # Schedule the next lease check
  defp schedule_lease_check(interval) do
    Process.send_after(self(), :check_lease_changes, interval)
  end

  # Core logic for checking lease changes
  defp check_for_lease_changes(state) do
    # Get all leases currently assigned to this worker
    my_leases = AppState.list_worker_leases(state.app_name, state.worker_id)

    # Convert to map for easier lookups
    my_lease_map =
      Enum.reduce(my_leases, %{}, fn lease, acc ->
        Map.put(acc, lease.shard_id, lease)
      end)

    # 1. Find new leases assigned to us
    new_leases =
      Enum.filter(my_leases, fn lease ->
        not Map.has_key?(state.current_leases, lease.shard_id)
      end)

    # 2. Find leases no longer assigned to us
    old_leases =
      Enum.filter(Map.keys(state.current_leases), fn shard_id ->
        case Map.get(my_lease_map, shard_id) do
          nil ->
            # Lease not in current assignments
            true

          %ShardLease{completed: true} ->
            # Lease is now completed
            true

          _ ->
            # Lease still assigned and not completed
            false
        end
      end)

    # Start processing for new leases
    Enum.each(new_leases, fn lease ->
      handle_new_lease(lease, state)
    end)

    # Stop processing for old leases
    Enum.each(old_leases, fn shard_id ->
      handle_old_lease(shard_id, state)
    end)

    # Create new state with updated lease tracking
    new_state = %{
      state
      | current_leases: my_lease_map,
        poll_timer: schedule_lease_check(state.poll_interval_ms)
    }

    new_state
  end

  # Handle a newly assigned lease
  defp handle_new_lease(lease, state) do
    shard_id = lease.shard_id
    Logger.info("New lease assigned for shard #{shard_id}")

    # Check if we're already running this shard
    existing_ref = Enum.find(state.shard_ref_map, fn {_ref, id} -> id == shard_id end)

    case existing_ref do
      nil ->
        # Start the shard
        case start_shard(shard_id, state) do
          {:ok, _pid} ->
            Logger.info("Started processing for shard #{shard_id}")

          error ->
            Logger.error("Failed to start shard #{shard_id}: #{inspect(error)}")
        end

      _ ->
        Logger.debug("Shard #{shard_id} is already being processed")
    end
  end

  # Handle a lease that's no longer assigned to us
  defp handle_old_lease(shard_id, state) do
    Logger.info("Lease no longer assigned for shard #{shard_id}")

    # Stop the shard process if it's running
    stop_shard(state.stream_name, shard_id)

    # Clean up references
    {ref, _} = remove_shard_reference(shard_id, state.shard_ref_map)
    if ref, do: Process.demonitor(ref, [:flush])
  end

  # Start a shard process
  defp start_shard(shard_id, %{shard_supervisor_name: supervisor, shard_args: base_args} = state) do
    shard_args =
      base_args
      |> Keyword.put(:shard_id, shard_id)
      |> Keyword.put(:shard_name, Shard.name(state.stream_name, shard_id))

    case Shard.start(supervisor, shard_args) do
      {:ok, _pid} = result ->
        Logger.info("Successfully started shard #{shard_id}")
        Logger.info("Broadway processes parent started")
        result

      {:error, {:already_started, pid}} ->
        Logger.info("Shard #{shard_id} is already started with pid #{inspect(pid)}")
        {:ok, pid}

      error ->
        Logger.error("Failed to start shard #{shard_id}: #{inspect(error)}")
        error
    end
  end

  # Stop a shard process
  def stop_shard(stream_name, shard_id) do
    shard_name = Shard.name(stream_name, shard_id)

    case Process.whereis(shard_name) do
      nil ->
        Logger.debug("Shard #{shard_id} is already stopped")
        :ok

      pid ->
        if Process.alive?(pid) do
          Logger.info("Stopping shard #{shard_id}")

          try do
            Shard.stop(shard_name)
            Logger.info("Successfully stopped shard #{shard_id}")
            :ok
          catch
            _kind, reason ->
              Logger.warning("Error stopping shard #{shard_id}: #{inspect(reason)}")
              stop_shard(stream_name, shard_id)
              :error
          end
        else
          Logger.info("Shard process #{shard_id} is not alive, cleaning up registry")
          :ok
        end
    end
  end

  # Helper function to restart a shard and update the reference map
  defp restart_shard(shard_id, state, cleaned_map) do
    case start_shard(shard_id, state) do
      {:ok, pid} ->
        new_ref = Process.monitor(pid)
        Logger.info("Restarted shard #{shard_id} with pid #{inspect(pid)}")
        %{state | shard_ref_map: Map.put(cleaned_map, new_ref, shard_id)}

      error ->
        Logger.error("Failed to restart shard #{shard_id}: #{inspect(error)}")
        %{state | shard_ref_map: cleaned_map}
    end
  end

  # Helper to find and remove a shard reference
  defp remove_shard_reference(shard_id, shard_ref_map) do
    case Enum.find(shard_ref_map, fn {_ref, id} -> id == shard_id end) do
      {ref, _id} -> {ref, Map.delete(shard_ref_map, ref)}
      nil -> {nil, shard_ref_map}
    end
  end

  # Generate process name
  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
