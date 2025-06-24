defmodule KinesisClient.Worker.Dynamo do
  @moduledoc false
  @behaviour KinesisClient.Worker.Adapter

  alias ExAws.Dynamo
  require Logger

  @registry_table_suffix "_worker_registry"
  @worker_timeout_ms 60_000

  @impl KinesisClient.Worker.Adapter
  def initialize(app_name, opts) do
    table_name = registry_table_name(app_name)
    ensure_registry_table_exists(table_name, opts)
  end

  @impl KinesisClient.Worker.Adapter
  def register_worker(app_name, worker_id, metadata, opts) do
    table_name = registry_table_name(app_name)
    now = System.system_time(:millisecond)

    item =
      Map.merge(metadata, %{
        "worker_id" => worker_id,
        "last_heartbeat" => now,
        "is_active" => true
      })

    case Dynamo.put_item(table_name, item) |> ExAws.request(opts) do
      {:ok, _} -> :ok
      error -> {:error, error}
    end
  end

  @impl KinesisClient.Worker.Adapter
  def heartbeat(app_name, worker_id, opts) do
    table_name = registry_table_name(app_name)
    now = System.system_time(:millisecond)

    update_opts = [
      update_expression: "SET last_heartbeat = :now, is_active = :active",
      expression_attribute_values: %{
        now: now,
        active: true
      }
    ]

    case Dynamo.update_item(table_name, %{"worker_id" => worker_id}, update_opts)
         |> ExAws.request(opts) do
      {:ok, _} -> :ok
      error -> {:error, error}
    end
  end

  @impl KinesisClient.Worker.Adapter
  def list_active_workers(app_name, opts) do
    table_name = registry_table_name(app_name)
    cutoff_time = System.system_time(:millisecond) - @worker_timeout_ms

    case list_workers_from_registry(table_name, opts) do
      {:ok, workers} ->
        workers
        |> Enum.filter(fn worker ->
          last_heartbeat =
            case worker["last_heartbeat"] do
              %{"N" => timestamp_string} when is_binary(timestamp_string) ->
                {int_val, _} = Integer.parse(timestamp_string)
                int_val

              timestamp when is_integer(timestamp) ->
                timestamp

              _ ->
                0
            end

          is_inactive =
            case worker["is_active"] do
              %{"BOOL" => false} -> true
              false -> true
              _ -> false
            end

          last_heartbeat >= cutoff_time && !is_inactive
        end)
        |> Enum.map(fn worker ->
          extract_worker_id(worker["worker_id"])
        end)

      {:error, reason} ->
        Logger.error("Failed to list active registry workers: #{inspect(reason)}")
        []
    end
  end

  @impl KinesisClient.Worker.Adapter
  def list_all_workers(app_name, opts) do
    table_name = registry_table_name(app_name)

    case list_workers_from_registry(table_name, opts) do
      {:ok, workers} ->
        workers

      {:error, reason} ->
        Logger.error("Failed to list all workers: #{inspect(reason)}")
        []
    end
  end

  @impl KinesisClient.Worker.Adapter
  def remove_worker(app_name, worker_id, opts) do
    table_name = registry_table_name(app_name)
    raw_worker_id = extract_worker_id(worker_id)

    key = %{"worker_id" => raw_worker_id}

    Logger.debug("Deleting worker with key: #{inspect(key)}")

    case Dynamo.delete_item(table_name, key) |> ExAws.request(opts) do
      {:ok, _} ->
        Logger.info("Successfully removed worker: #{worker_id}")
        :ok

      {:error, error} ->
        Logger.error("Failed to delete worker #{worker_id}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl KinesisClient.Worker.Adapter
  def get_worker(app_name, worker_id, opts) do
    table_name = registry_table_name(app_name)

    case Dynamo.get_item(table_name, %{"worker_id" => worker_id}) |> ExAws.request(opts) do
      {:ok, %{"Item" => item}} -> {:ok, item}
      {:ok, %{}} -> :not_found
      error -> {:error, error}
    end
  end

  # Private helper functions

  defp registry_table_name(app_name), do: "#{app_name}#{@registry_table_suffix}"

  defp ensure_registry_table_exists(table_name, opts) do
    case Dynamo.describe_table(table_name) |> ExAws.request(opts) do
      {:ok, _} ->
        :ok

      # TODO: Add retry limit and test process failure.
      {:error, {"ResourceNotFoundException", _}} ->
        create_registry_table(table_name, opts)

      {:error, reason} ->
        create_registry_table(table_name, opts)
        {:error, reason}
    end
  end

  defp create_registry_table(table_name, opts) do
    Logger.info("Creating worker registry table: #{table_name}")

    hash_key = "worker_id"
    hash_key_type = :string
    read_capacity = 5
    write_capacity = 5

    case Dynamo.create_table(
           table_name,
           hash_key,
           %{worker_id: hash_key_type},
           read_capacity,
           write_capacity
         )
         |> ExAws.request(opts) do
      {:ok, _} ->
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
      case Dynamo.describe_table(table_name) |> ExAws.request(opts) do
        {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} ->
          :ok

        {:ok, _} ->
          Process.sleep(delay)
          wait_for_table_active(table_name, opts, retries - 1, delay)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp list_workers_from_registry(table_name, opts) do
    case Dynamo.scan(table_name) |> ExAws.request(opts) do
      {:ok, %{"Items" => items}} -> {:ok, items}
      error -> {:error, error}
    end
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
end
