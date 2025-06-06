defmodule KinesisClient.Worker.Adapter do
  @moduledoc """
  This interface specifies what the WorkerRegistry needs to support.
  """

  @doc """
  Initialize the worker registry backend storage.
  """
  @callback initialize(app_name :: String.t(), opts :: Keyword.t()) :: :ok | {:error, any}

  @doc """
  Register a worker with metadata in the registry.
  """
  @callback register_worker(
              app_name :: String.t(),
              worker_id :: String.t(),
              metadata :: map(),
              opts :: Keyword.t()
            ) :: :ok | {:error, any}

  @doc """
  Send a heartbeat for a worker to indicate it's still active.
  """
  @callback heartbeat(
              app_name :: String.t(),
              worker_id :: String.t(),
              opts :: Keyword.t()
            ) :: :ok | {:error, any}

  @doc """
  List all active workers based on recent heartbeats.
  """
  @callback list_active_workers(app_name :: String.t(), opts :: Keyword.t()) ::
              [String.t()] | {:error, any}

  @doc """
  List all workers regardless of activity status.
  """
  @callback list_all_workers(app_name :: String.t(), opts :: Keyword.t()) ::
              [map()] | {:error, any}

  @doc """
  Remove a worker from the registry.
  """
  @callback remove_worker(
              app_name :: String.t(),
              worker_id :: String.t(),
              opts :: Keyword.t()
            ) :: :ok | {:error, any}

  @doc """
  Get information about a specific worker.
  """
  @callback get_worker(
              app_name :: String.t(),
              worker_id :: String.t(),
              opts :: Keyword.t()
            ) :: {:ok, map()} | :not_found | {:error, any}
end
