defmodule KinesisClient.Stream.LeaseRefresher do
  @moduledoc """
  This module coordinates lease taking and lease renewal in a KCL-style approach.

  The LeaseRefresher is responsible for renewing leases

  Based on the Amazon KCL LeaseRefresher functionality.
  """
  use GenServer
  require Logger
  alias KinesisClient.Stream.AppState

  # Take leases every 20 seconds
  @default_lease_take_interval 20_000
  # Renew leases every 10 seconds
  @default_lease_renew_interval 10_000

  # Client API

  @doc """
  Starts the LeaseRefresher.

  ## Options
    * `:app_name` - Required. The name of the application/stream.
    * `:worker_id` - Required. The ID of the current worker.
    * `:app_state_opts` - Options for the app state store.
    * `:lease_renew_interval` - Interval in milliseconds between lease renewal operations. Default: 10,000ms.
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
    app_state_opts = Keyword.get(opts, :app_state_opts, [])
    lease_take_interval = Keyword.get(opts, :lease_take_interval, @default_lease_take_interval)
    lease_renew_interval = Keyword.get(opts, :lease_renew_interval, @default_lease_renew_interval)

    Logger.info("Starting LeaseRefresher for #{app_name}, worker: #{worker_id}")

    # Immediately schedule the first lease take and renewal operations
    renew_timer = schedule_lease_renewal(lease_renew_interval)

    {:ok,
     %{
       app_name: app_name,
       worker_id: worker_id,
       app_state_opts: app_state_opts,
       lease_take_interval: lease_take_interval,
       lease_renew_interval: lease_renew_interval,
       renew_timer: renew_timer,
       take_timer: nil,
       running: true
     }}
  end

  @impl GenServer
  def handle_info(:renew_leases, state) do
    Logger.info("Renewing leases for #{state.worker_id}", ansi_color: :green_background)

    if state.running do
      # Cancel existing timer
      if state.renew_timer, do: Process.cancel_timer(state.renew_timer)

      # Execute lease renewal
      results =
        renew_leases(
          state.app_name,
          state.worker_id
        )

      # Log results
      success_count = length(results.success)
      failure_count = length(results.failure)

      if failure_count > 0 do
        Logger.warning("Renewed #{success_count} leases, failed to renew #{failure_count} leases")
      else
        Logger.debug("Successfully renewed #{success_count} leases", ansi_color: :green)
      end

      # Schedule next lease renewal
      renew_timer = schedule_lease_renewal(state.lease_renew_interval)
      {:noreply, %{state | renew_timer: renew_timer}}
    else
      {:noreply, state}
    end
  end

  defp schedule_lease_renewal(interval) do
    Process.send_after(self(), :renew_leases, interval)
  end

  defp name(app_name) do
    :"#{__MODULE__}.#{app_name}"
  end

  @doc """
  Renews all leases currently held by this worker.

  Returns a map with two keys:
  - :success - List of shard IDs for which lease renewal was successful
  - :failure - List of shard IDs for which lease renewal failed
  """
  defp renew_leases(app_name, worker_id) do
    Logger.debug("Renewing leases for worker #{worker_id}")

    # Get all leases currently held by this worker
    current_leases = AppState.list_worker_leases(app_name, worker_id)

    # Track successful and failed renewals
    results = %{
      success: [],
      failure: []
    }

    # Renew each lease
    Enum.reduce(current_leases, results, fn lease, acc ->
      case AppState.renew_lease(app_name, lease) do
        {:ok, _new_count} ->
          # Successfully renewed lease
          %{acc | success: [lease.shard_id | acc.success]}

        {:error, _reason} ->
          # Failed to renew lease
          Logger.warning("Failed to renew lease for shard #{lease.shard_id}")
          %{acc | failure: [lease.shard_id | acc.failure]}

        error ->
          # Unexpected error
          Logger.error("Error renewing lease for shard #{lease.shard_id}: #{inspect(error)}")
          %{acc | failure: [lease.shard_id | acc.failure]}
      end
    end)
  end
end
