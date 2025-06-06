defmodule KinesisClient.Worker.Service do
  @moduledoc """
  Service that manages worker registration and cleanup logic.
  Contains the business logic for worker registry operations.
  """

  use GenServer
  require Logger
  alias KinesisClient.WorkerRegistry
  alias KinesisClient.LeaderElection

  @heartbeat_interval_ms 5_000
  @cleanup_interval_ms 30_000
  @worker_timeout_ms 60_000

  # Client API

  @doc """
  Starts the worker registry service.

  ## Options
    * `:app_name` - Required. Used to name the registry table
    * `:worker_id` - Required. Unique identifier for this worker
    * `:dynamo_opts` - Optional. Options to pass to ExAws.Dynamo
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    GenServer.start_link(__MODULE__, opts, name: name(app_name))
  end

  # @doc """
  # Manually forces cleanup of a specific worker by ID.
  # This is useful for removing workers that have leases but no registry entry.
  # """
  # def force_cleanup_worker(app_name, worker_id, opts \\ []) do
  #   GenServer.cast(name(app_name), {:force_cleanup_worker, worker_id, opts})
  # end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)
    dynamo_opts = Keyword.get(opts, :dynamo_opts, [])

    # Initialize the worker registry
    :ok = WorkerRegistry.initialize(app_name, dynamo_opts)

    # Schedule first heartbeat
    schedule_heartbeat(heartbeat_interval_ms())

    # Schedule first cleanup
    schedule_cleanup(cleanup_interval_ms())

    {:ok,
     %{
       app_name: app_name,
       worker_id: worker_id,
       dynamo_opts: dynamo_opts,
       last_heartbeat: nil
     }}
  end

  @impl GenServer
  def handle_info(:heartbeat, state) do
    # Update registry with heartbeat
    result = WorkerRegistry.heartbeat(state.app_name, state.worker_id, state.dynamo_opts)

    case result do
      :ok ->
        Logger.debug("Successfully sent worker registry heartbeat for #{state.worker_id}")

      {:error, reason} ->
        Logger.warning("Failed to update worker registry: #{inspect(reason)}")
    end

    # Schedule next heartbeat
    schedule_heartbeat(heartbeat_interval_ms())

    {:noreply, %{state | last_heartbeat: System.system_time(:millisecond)}}
  end

  @impl GenServer
  def handle_info(:cleanup_stale_workers, state) do
    if KinesisClient.LeaderElection.is_leader?(state.app_name) do
      # Only the leader cleans up stale workers
      Logger.debug("Cleaning up stale workers from registry")

      # Calculate cutoff time for worker expiration
      cutoff_time = System.system_time(:millisecond) - worker_timeout_ms()

      # Get all workers
      all_workers = WorkerRegistry.list_all_workers(state.app_name, state.dynamo_opts)
      total_workers = length(all_workers)

      # Filter for stale workers
      stale_workers =
        Enum.filter(all_workers, fn worker ->
          # Extract last_heartbeat value, converting from DynamoDB format if needed
          last_heartbeat =
            case worker["last_heartbeat"] do
              %{"N" => timestamp_string} when is_binary(timestamp_string) ->
                {int_val, _} = Integer.parse(timestamp_string)
                int_val

              timestamp when is_integer(timestamp) ->
                timestamp

              # Default to 0 (very old) if invalid format
              _ ->
                0
            end

          worker_id = extract_worker_id(worker["worker_id"])

          # Check if the worker has an is_active flag set to false
          is_inactive =
            case worker["is_active"] do
              %{"BOOL" => false} -> true
              false -> true
              _ -> false
            end

          # Now safely compare as integers for stale check
          is_stale = last_heartbeat < cutoff_time || is_inactive

          # Calculate time difference for logging
          time_since_heartbeat =
            if is_integer(last_heartbeat) do
              System.system_time(:millisecond) - last_heartbeat
            else
              # If we can't calculate, just show a placeholder
              "unknown"
            end

          if is_stale do
            if is_inactive do
              Logger.info("Worker #{worker_id} is marked as inactive")
            else
              Logger.info(
                "Worker #{worker_id} is stale - last heartbeat #{time_since_heartbeat}ms ago (threshold: #{worker_timeout_ms()}ms)"
              )
            end
          end

          is_stale
        end)

      stale_count = length(stale_workers)
      Logger.info("Found #{stale_count} stale workers out of #{total_workers} total")

      # Remove stale workers
      if stale_count > 0 do
        stale_worker_ids =
          Enum.map(stale_workers, fn w -> extract_worker_id(w["worker_id"]) end)

        Logger.info("Removing stale workers: #{inspect(stale_worker_ids)}")

        Enum.each(stale_workers, fn worker ->
          worker_id = extract_worker_id(worker["worker_id"])
          WorkerRegistry.remove_worker(state.app_name, worker_id, state.dynamo_opts)
        end)

        # Verify cleanup by immediately rechecking the registry
        verify_cleanup(state.app_name, stale_worker_ids, state.dynamo_opts)
      end
    end

    # Schedule next cleanup
    schedule_cleanup(cleanup_interval_ms())

    {:noreply, state}
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :heartbeat, interval)
  end

  defp schedule_cleanup(interval) do
    Process.send_after(self(), :cleanup_stale_workers, interval)
  end

  # Helper function to extract worker_id from DynamoDB format
  defp extract_worker_id(worker_id) do
    case worker_id do
      %{"S" => id} when is_binary(id) ->
        id

      id when is_binary(id) ->
        id

      other ->
        # For logging or debugging purposes, safely convert to string
        inspect(other)
    end
  end

  defp verify_cleanup(app_name, worker_ids, dynamo_opts) do
    # Give DynamoDB a moment to process deletions
    Process.sleep(500)

    all_workers = WorkerRegistry.list_all_workers(app_name, dynamo_opts)

    # Check if any stale workers still exist
    remaining_stale =
      Enum.filter(all_workers, fn worker ->
        current_id = extract_worker_id(worker["worker_id"])
        # Only retry if it's in our list of workers to clean up
        Enum.member?(worker_ids, current_id)
      end)

    if length(remaining_stale) > 0 do
      remaining_ids = Enum.map(remaining_stale, fn w -> extract_worker_id(w["worker_id"]) end)

      Logger.warning(
        "Cleanup verification: #{length(remaining_stale)} stale workers still exist: #{inspect(remaining_ids)}"
      )

      # Try again to remove any remaining stale workers
      Enum.each(remaining_stale, fn worker ->
        display_id = extract_worker_id(worker["worker_id"])
        Logger.info("Retry removing worker: #{display_id}")
        WorkerRegistry.remove_worker(app_name, display_id, dynamo_opts)
      end)

      Logger.info("Forced deletion of all remaining stale workers")
    else
      Logger.info("Cleanup verification: All stale workers successfully removed")
    end
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"

  defp heartbeat_interval_ms do
    Application.get_env(:kcl_ex, :heartbeat_interval_ms, @heartbeat_interval_ms)
  end

  defp cleanup_interval_ms do
    Application.get_env(:kcl_ex, :cleanup_interval_ms, @cleanup_interval_ms)
  end

  defp worker_timeout_ms do
    Application.get_env(:kcl_ex, :worker_timeout_ms, @worker_timeout_ms)
  end
end
