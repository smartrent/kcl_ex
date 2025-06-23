defmodule KinesisClient.LeaseBalancer do
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
