defmodule KinesisClient.WorkerRegistry do
  @moduledoc """
  Manages worker registration and discovery.
  """

  @doc """
  Initializes the worker registry backend storage.

  ## Options
    * `:adapter` - Optional. The adapter module to use. Defaults to KinesisClient.Worker.Dynamo.
  """
  def initialize(app_name, opts \\ []) do
    adapter(opts).initialize(app_name, opts)
  end

  @doc """
  Registers a worker with metadata in the registry.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def register_worker(app_name, worker_id, metadata \\ %{}, opts \\ []) do
    adapter(opts).register_worker(app_name, worker_id, metadata, opts)
  end

  @doc """
  Sends a heartbeat for a worker to indicate it's still active.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def heartbeat(app_name, worker_id, opts \\ []) do
    adapter(opts).heartbeat(app_name, worker_id, opts)
  end

  @doc """
  Lists all active workers based on recent heartbeats.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def list_active_workers(app_name, opts \\ []) do
    adapter(opts).list_active_workers(app_name, opts)
  end

  @doc """
  Lists all workers regardless of activity status.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def list_all_workers(app_name, opts \\ []) do
    adapter(opts).list_all_workers(app_name, opts)
  end

  @doc """
  Removes a worker from the registry.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def remove_worker(app_name, worker_id, opts \\ []) do
    adapter(opts).remove_worker(app_name, worker_id, opts)
  end

  @doc """
  Gets information about a specific worker.

  ## Options
    * `:adapter` - Optional. The adapter module to use.
  """
  def get_worker(app_name, worker_id, opts \\ []) do
    adapter(opts).get_worker(app_name, worker_id, opts)
  end

  defp adapter(opts) do
    Keyword.get(opts, :adapter, KinesisClient.Worker.Dynamo)
  end
end
