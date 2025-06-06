defmodule KinesisClient.LeaderElection do
  @moduledoc """
  Handles leader election for KinesisClient workers using DynamoDB.

  This module implements a leader election protocol similar to the one in AWS KCL 3.x:
  - Uses a DynamoDB lock-based approach similar to DynamoDBLockClient
  - Each worker tries to take leadership periodically
  - Only one worker can be the leader at a time
  - Leader sends heartbeats to maintain leadership
  - If leader fails, another worker can take over after heartbeat expires
  - Provides mechanism for consistent leader failure handling
  """
  use GenServer
  require Logger

  alias KinesisClient.Leadership.AppState

  @leader_table_suffix "_leader_lock"
  # 30 seconds lease duration, similar to KCL 3.x
  @leader_lease_duration_ms 30_000
  # 3 seconds heartbeat interval - more frequent than expiry for reliability
  @heartbeat_interval_ms 3_000
  # 5 seconds grace period before takeover attempts
  @takeover_grace_period_ms 5_004
  # Track consecutive failures for leader abandonment
  @max_consecutive_failures 3

  # Client API

  @doc """
  Starts the leader election process.

  ## Options
    * `:app_name` - Required. Used to name the leader table
    * `:worker_id` - Required. Unique identifier for this worker
    * `:dynamo_opts` - Optional. Options to pass to ExAws.Dynamo
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(opts[:app_name]))
  end

  @doc """
  Returns whether the current worker is the leader.
  """
  def is_leader?(app_name) do
    GenServer.call(name(app_name), :is_leader?)
  end

  @doc """
  Abandons leadership if the current worker is the leader.
  Used in failure scenarios where the leader wants to gracefully relinquish control.
  """
  def abandon_leadership(app_name) do
    GenServer.cast(name(app_name), :abandon_leadership)
  end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)
    dynamo_opts = Keyword.get(opts, :dynamo_opts, [])

    # Generate leader table name
    leader_table = leader_table_name(app_name)

    # Ensure leader table exists
    case ensure_leader_table_exists(leader_table, dynamo_opts) do
      :ok ->
        Logger.info("Leader table #{leader_table} ready")

      {:error, reason} ->
        Logger.error("Failed to initialize leader table: #{inspect(reason)}")
    end

    # Schedule first leadership check
    Process.send_after(self(), :check_leadership, 1_000)

    {:ok,
     %{
       app_name: app_name,
       leader_table: leader_table,
       worker_id: worker_id,
       dynamo_opts: dynamo_opts,
       is_leader: false,
       current_leader: nil,
       heartbeat_timer: nil,
       leadership_check_timer: nil,
       consecutive_failures: 0
     }}
  end

  @impl GenServer
  def handle_info(:check_leadership, state) do
    # Cancel any existing timer
    if state.leadership_check_timer do
      Process.cancel_timer(state.leadership_check_timer)
    end

    # Get current leader info
    leader_info = get_leader_info(state.leader_table, state.dynamo_opts)

    new_state =
      case leader_info do
        # No current leader - try to become leader
        nil ->
          Logger.info("No current leader, attempting leadership acquisition")
          attempt_leadership_acquisition(state)

        # Check if I'm already the leader
        %{"worker_id" => db_worker_id} ->
          # Extract the worker_id if it's in DynamoDB format
          worker_id_str = extract_string(db_worker_id)

          if worker_id_str == state.worker_id do
            # I'm the leader - continue heartbeating
            # Check if our leadership record is recent enough
            current_time = System.system_time(:millisecond)
            last_update = Map.get(leader_info, "last_update", 0)
            last_update_value = extract_number(last_update)

            # If our recorded leadership is stale, we might have been usurped
            if current_time - last_update_value > @leader_lease_duration_ms do
              Logger.warning("Our leadership record is stale, checking if we're still leader")
              attempt_leadership_acquisition(state)
            else
              # We're still the leader, ensure heartbeat is scheduled
              Logger.debug("I'm the leader, ensuring heartbeat is scheduled")
              ensure_heartbeat_scheduled(state)
            end
          else
            # Another worker is the leader - check if it's expired
            current_time = System.system_time(:millisecond)
            last_update = Map.get(leader_info, "last_update", 0)
            last_update_value = extract_number(last_update)

            leader_expired =
              current_time - last_update_value >
                @leader_lease_duration_ms + @takeover_grace_period_ms

            if leader_expired do
              # The leader has expired, try to take over
              Logger.info("Leader #{worker_id_str} expired, attempting takeover")
              attempt_leadership_acquisition(state)
            else
              # Leader is still active, schedule next check
              timer = Process.send_after(self(), :check_leadership, @heartbeat_interval_ms * 2)

              # Reset consecutive failures counter as things are working normally
              %{
                state
                | is_leader: false,
                  current_leader: worker_id_str,
                  leadership_check_timer: timer,
                  consecutive_failures: 0
              }
            end
          end
      end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info(:send_heartbeat, state) do
    # Cancel existing timer
    if state.heartbeat_timer do
      Process.cancel_timer(state.heartbeat_timer)
    end

    # Only send heartbeat if we're the leader
    if state.is_leader do
      current_time = System.system_time(:millisecond)

      case AppState.update_leader_heartbeat(
             state.leader_table,
             state.worker_id,
             current_time,
             state.dynamo_opts
           ) do
        {:ok, _} ->
          # Successfully sent heartbeat
          Logger.debug("Sent leadership heartbeat")
          # Reset the consecutive failures counter
          new_state = %{state | consecutive_failures: 0}

          # Schedule next heartbeat
          timer = Process.send_after(self(), :send_heartbeat, @heartbeat_interval_ms)
          {:noreply, %{new_state | heartbeat_timer: timer}}

        {:error, reason} ->
          # Failed to send heartbeat - we may have lost leadership
          Logger.warning("Failed to send leadership heartbeat: #{inspect(reason)}")

          # Increment consecutive failures counter
          new_failures = state.consecutive_failures + 1

          if new_failures >= @max_consecutive_failures do
            # If we've failed too many times, abandon leadership
            Logger.error(
              "Too many consecutive heartbeat failures (#{new_failures}), abandoning leadership"
            )

            Process.send_after(self(), :abandon_leadership, 0)
            {:noreply, %{state | consecutive_failures: new_failures, heartbeat_timer: nil}}
          else
            # Otherwise, try again
            Logger.warning(
              "Heartbeat failure #{new_failures}/#{@max_consecutive_failures}, will retry"
            )

            # Check leadership status after a short delay
            Process.send_after(self(), :check_leadership, 500)
            {:noreply, %{state | consecutive_failures: new_failures, heartbeat_timer: nil}}
          end
      end
    else
      Logger.debug("Not the leader, skipping heartbeat")
      # We're not the leader anymore
      {:noreply, %{state | heartbeat_timer: nil}}
    end
  end

  @impl GenServer
  def handle_call(:is_leader?, _from, state) do
    {:reply, state.is_leader, state}
  end

  @impl GenServer
  def handle_cast(:abandon_leadership, state) do
    if state.is_leader do
      Logger.warning("Abandoning leadership due to request or consecutive failures")

      # Release the lock by removing the leader record
      case AppState.delete_leader_item(state.leader_table, state.worker_id, state.dynamo_opts) do
        {:ok, _} ->
          Logger.info("Successfully released leadership lock")

        {:error, reason} ->
          Logger.warning("Failed to release leadership lock: #{inspect(reason)}")
      end

      # Cancel heartbeat timer
      if state.heartbeat_timer do
        Process.cancel_timer(state.heartbeat_timer)
      end

      # Set state to non-leader and schedule a new leadership check
      timer = Process.send_after(self(), :check_leadership, @heartbeat_interval_ms * 2)

      {:noreply,
       %{
         state
         | is_leader: false,
           heartbeat_timer: nil,
           leadership_check_timer: timer,
           consecutive_failures: 0
       }}
    else
      # Not the leader, nothing to do
      {:noreply, state}
    end
  end

  # Private functions

  defp attempt_leadership_acquisition(state) do
    current_time = System.system_time(:millisecond)

    case AppState.put_leader_item(
           state.leader_table,
           state.worker_id,
           current_time,
           state.dynamo_opts
         ) do
      {:ok, _} ->
        # Successfully became leader
        Logger.info("Worker #{inspect(state.worker_id)} acquired leadership")

        # Start heartbeating
        timer = Process.send_after(self(), :send_heartbeat, @heartbeat_interval_ms)

        # Also schedule a leadership check as a backup
        check_timer =
          Process.send_after(
            self(),
            :check_leadership,
            @leader_lease_duration_ms
          )

        %{
          state
          | is_leader: true,
            current_leader: state.worker_id,
            heartbeat_timer: timer,
            leadership_check_timer: check_timer,
            consecutive_failures: 0
        }

      {:error, reason} ->
        # Failed to become leader
        Logger.debug("Failed to acquire leadership: #{inspect(reason)}")

        # Schedule another check
        timer = Process.send_after(self(), :check_leadership, @heartbeat_interval_ms * 2)
        %{state | is_leader: false, leadership_check_timer: timer}
    end
  end

  defp ensure_heartbeat_scheduled(%{heartbeat_timer: nil} = state) do
    timer = Process.send_after(self(), :send_heartbeat, @heartbeat_interval_ms)
    # Also schedule a leadership check as a backup
    check_timer =
      Process.send_after(
        self(),
        :check_leadership,
        @leader_lease_duration_ms
      )

    %{state | heartbeat_timer: timer, leadership_check_timer: check_timer}
  end

  defp ensure_heartbeat_scheduled(state), do: state

  defp get_leader_info(table_name, opts) do
    case AppState.get_leader_info(table_name, opts) do
      {:ok, %{"Item" => item}} ->
        item

      {:ok, %{}} ->
        nil

      {:error, {"ResourceNotFoundException", _}} ->
        nil

      {:error, reason} ->
        Logger.error("Error getting leader info: #{inspect(reason)}")
        nil
    end
  end

  defp ensure_leader_table_exists(table_name, opts) do
    # Check if table exists
    case AppState.describe_table(table_name, opts) do
      {:ok, _} ->
        # Table exists
        :ok

      {:error, {"ResourceNotFoundException", _}} ->
        # Create the table
        create_leader_table(table_name, opts)

      {:error, reason} ->
        # Other error
        {:error, reason}
    end
  end

  defp create_leader_table(table_name, opts) do
    Logger.info("Creating leader election table: #{table_name}")

    # Extract required parameters for the table
    hash_key = "leader_key"
    # "S" in DynamoDB terms
    key_schema = %{leader_key: :string}
    read_capacity = 5
    write_capacity = 5

    case AppState.create_table(
           table_name,
           hash_key,
           key_schema,
           read_capacity,
           write_capacity,
           opts
         ) do
      {:ok, _} ->
        # Table created, wait for it to become active
        wait_for_table_active(table_name, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp wait_for_table_active(table_name, opts, retries \\ 10, delay \\ 1000) do
    if retries <= 0 do
      Logger.error("Timed out waiting for table #{table_name} to become active")
      {:error, :timeout}
    else
      case AppState.describe_table(table_name, opts) do
        {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} ->
          :ok

        {:ok, _} ->
          # Table exists but not active yet
          Process.sleep(delay)
          wait_for_table_active(table_name, opts, retries - 1, delay)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp leader_table_name(app_name), do: "#{app_name}#{@leader_table_suffix}"

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"

  # Helper function to safely extract numbers from DynamoDB responses
  defp extract_number(value) when is_integer(value), do: value

  defp extract_number(%{"N" => string_value}) when is_binary(string_value),
    do: String.to_integer(string_value)

  defp extract_number(value) when is_binary(value), do: String.to_integer(value)
  defp extract_number(_), do: 0

  # Helper function to safely extract strings from DynamoDB responses
  defp extract_string(value) when is_binary(value), do: value
  defp extract_string(%{"S" => string_value}) when is_binary(string_value), do: string_value
  defp extract_string(value), do: inspect(value)
end
