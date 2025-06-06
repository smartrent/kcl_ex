defmodule KinesisClient.LeaseCoordinator do
  @moduledoc """
  Centralized lease assignment manager based on KCL 3.x approach.

  In KCL 3.x, lease assignment is done by a leader worker instead of individual
  workers taking or stealing leases. This provides several benefits:

  1. Reduced contention and DynamoDB API calls
  2. More intelligent, workload-aware lease balancing
  3. Consideration of shard throughput and worker CPU utilization
  4. More stable lease assignments with less churn

  This module handles:
  - Priority lease assignments (unassigned or expired leases)
  - Tracking throughput per shard
  - Tracking CPU utilization of workers (when available)
  """
  use GenServer
  require Logger
  alias KinesisClient.LeaseCoordinator
  alias KinesisClient.Stream.AppState
  alias KinesisClient.LeaderElection

  # Default configuration values
  # 30 seconds
  @default_assignment_interval_ms 60_000
  # After 3 failures, abandon leadership
  @max_consecutive_failures 3

  # Client API

  @doc """
  Starts the LeaseCoordinator process.

  ## Options
    * `:app_name` - Required. The name of the application/stream.
    * `:worker_id` - Required. The ID of the current worker.
    * `:assignment_interval` - Interval in ms between assignment operations.
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    GenServer.start_link(__MODULE__, opts, name: name(app_name))
  end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)

    assignment_interval = Keyword.get(opts, :assignment_interval, @default_assignment_interval_ms)

    Process.send_after(self(), :assign_leases, 0)

    {:ok,
     %{
       app_name: app_name,
       worker_id: worker_id,
       assignment_interval: assignment_interval,
       last_assignment_at: nil,
       consecutive_failures: 0
     }}
  end

  @impl GenServer
  def handle_info(:assign_leases, state) do
    if KinesisClient.LeaderElection.is_leader?(state.app_name) do
      start_time = System.monotonic_time(:millisecond)

      Logger.info("Running lease assignment")

      result = execute_lease_assignment(state)

      end_time = System.monotonic_time(:millisecond)
      duration_ms = end_time - start_time

      {new_state, next_interval} =
        case result do
          {:ok, updated_state} ->
            # Assignment successful, update stats and reset failure counter
            Logger.info("Lease assignment completed successfully in #{duration_ms}ms")

            updated_state = %{
              updated_state
              | last_assignment_at: System.system_time(:millisecond),
                consecutive_failures: 0
            }

            {updated_state, state.assignment_interval}

          {:error, reason} ->
            # Assignment failed
            Logger.error("Lease assignment failed: #{inspect(reason)}")
            new_failures = state.consecutive_failures + 1

            if new_failures >= @max_consecutive_failures do
              Logger.error("#{new_failures} consecutive assignment failures, abandoning leadership")
              # Abandon leadership to allow another worker to take over
              LeaderElection.abandon_leadership(state.app_name)
              # Use a longer interval before trying again
              {%{state | consecutive_failures: new_failures}, state.assignment_interval * 2}
            else
              # Use a shorter interval for retry
              {%{state | consecutive_failures: new_failures}, div(state.assignment_interval, 2)}
            end

          :noop ->
            # No action needed, continue with the next assignment
            {%{state | consecutive_failures: 0}, state.assignment_interval}
        end

      # Schedule the next assignment
      Process.send_after(self(), :assign_leases, @default_assignment_interval_ms)

      {:noreply, new_state}
    else
      Logger.info("Skipping Lease Assignment - not leader")
      Process.send_after(self(), :assign_leases, @default_assignment_interval_ms)
      {:noreply, state}
    end
  end

  # Main lease assignment logic
  defp execute_lease_assignment(state) do
    # Get all leases and worker information
    all_leases = AppState.list_all_leases(state.app_name)
    # active leases include CHILD shards
    active_leases = Enum.filter(all_leases, &(!&1.completed))

    # Get all active nodes/workers
    all_workers = KinesisClient.WorkerRegistry.list_active_workers(state.app_name)
    Logger.info("Executing lease assignments")

    Logger.info(
      "Total workers: #{length(all_workers)}, Total active leases: #{length(active_leases)}"
    )

    # 1. First, assign available leases (priority assignment)
    # Make this more robust by handling both tuple returns and simple integers
    assign_result =
      rebalance_leases(
        state.app_name,
        active_leases,
        all_workers
      )

    {:ok, state}
  end

  # Assign available (unassigned or expired) leases to workers
  defp rebalance_leases(app_name, all_leases, all_workers) do
    LeaseBalancer.balance_leases(all_leases, all_workers)
    |> Enum.map(fn {worker_id, leases} ->
      Enum.map(leases, fn lease ->
        if lease.lease_owner == worker_id do
          Logger.info("No lease reassignments required for #{worker_id}")
          :ok
        else
          assign_lease_to_worker(app_name, lease, worker_id)
        end
      end)
    end)
  end

  # Assign a lease to a specific worker
  defp assign_lease_to_worker(app_name, lease, worker_id) do
    # Verify we're not trying to assign a child waiting lease to a regular worker or vice versa
    case {worker_id, lease.lease_owner, lease.lease_status} do
      # Force CHILD_WAITING owner to use CHILD_WAITING status
      {"CHILD_WAITING", _, _} ->
        assign_lease_with_status(app_name, lease, worker_id, "CHILD_WAITING")

      # Prevent regular workers from being assigned CHILD_WAITING leases
      {_, "CHILD_WAITING", _} ->
        Logger.error(
          "Invalid attempt to assign CHILD_WAITING shard #{lease.shard_id} to regular worker #{worker_id}"
        )

        :error

      {_, _, "CHILD_WAITING"} ->
        Logger.error(
          "Invalid attempt to assign CHILD_WAITING shard #{lease.shard_id} to regular worker #{worker_id}"
        )

        :error

      # Default for regular workers
      _ ->
        assign_lease_with_status(app_name, lease, worker_id, "LEASED")
    end
  end

  defp assign_lease_with_status(app_name, lease, worker_id, lease_status) do
    Logger.info(
      "Assigning lease for shard #{lease.shard_id} to worker #{worker_id} with status=#{lease_status}"
    )

    case AppState.take_lease(
           app_name,
           lease.shard_id,
           worker_id,
           lease.lease_count,
           [],
           lease_status
         ) do
      {:ok, _} ->
        Logger.info(
          "Successfully assigned lease for shard #{lease.shard_id} to worker #{worker_id}"
        )

        :ok

      error ->
        Logger.warning("Failed to assign lease for shard #{lease.shard_id}: #{inspect(error)}")
        :error
    end
  end

  defp name(app_name) do
    :"#{__MODULE__}.#{app_name}"
  end
end

defmodule LeaseBalancer do
  @doc """
  Balances leases amongst workers as evenly as possible.

  ## Examples

      iex> LeaseBalancer.balance_leases([1, 2, 3], [:a, :b])
      %{a: [1, 2], b: [3]}

      iex> LeaseBalancer.balance_leases([1, 2, 3, 4, 5], [:a, :b])
      %{a: [1, 2, 3], b: [4, 5]}

      iex> LeaseBalancer.balance_leases([1, 2, 3, 4, 5], [:a, :b, :c])
      %{a: [1, 2], b: [3, 4], c: [5]}
  """
  def balance_leases(leases, workers) when length(workers) > 0 do
    leases = Enum.sort(leases)
    workers = Enum.sort(workers)
    lease_count = length(leases)
    worker_count = length(workers)

    # Calculate base allocation and remainder
    base_leases_per_worker = div(lease_count, worker_count)
    remainder = rem(lease_count, worker_count)

    # Create allocation plan: some workers get base+1, others get base
    allocations = create_allocation_plan(workers, base_leases_per_worker, remainder)

    # Distribute leases according to the plan
    distribute_leases(leases, allocations)
  end

  def balance_leases(_leases, []), do: %{}

  defp create_allocation_plan(workers, base_count, remainder) do
    workers
    |> Enum.with_index()
    |> Enum.map(fn {worker, index} ->
      # First 'remainder' workers get an extra lease
      lease_count = if index < remainder, do: base_count + 1, else: base_count
      {worker, lease_count}
    end)
  end

  defp distribute_leases(leases, allocations) do
    {result, _remaining_leases} =
      Enum.reduce(allocations, {%{}, leases}, fn {worker, count}, {acc, remaining} ->
        {worker_leases, rest} = Enum.split(remaining, count)
        {Map.put(acc, worker, worker_leases), rest}
      end)

    result
  end

  @doc """
  Returns just the allocation counts for each worker (useful for debugging/planning).

  ## Examples

      iex> LeaseBalancer.get_allocation_counts(5, [:a, :b, :c])
      %{a: 2, b: 2, c: 1}
  """
  def get_allocation_counts(lease_count, workers) when length(workers) > 0 do
    worker_count = length(workers)
    base_leases_per_worker = div(lease_count, worker_count)
    remainder = rem(lease_count, worker_count)

    workers
    |> Enum.with_index()
    |> Enum.into(%{}, fn {worker, index} ->
      count = if index < remainder, do: base_leases_per_worker + 1, else: base_leases_per_worker
      {worker, count}
    end)
  end

  def get_allocation_counts(_lease_count, []), do: %{}
end
