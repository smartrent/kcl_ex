defmodule KinesisClient.Lease.Supervisor do
  @moduledoc """
  Supervisor for lease management components in KinesisClient.

  Manages hierarchical shard syncing, lease refreshing, and coordination
  following KCL 3.x approach.
  """
  use Supervisor
  require Logger

  @doc """
  Starts the leader election supervisor.

  ## Options
    * `:kinesis_stream_name` - Required. The name of the application
    * `:worker_id` - Required. Unique identifier for this worker
    * All other options are passed to the LeaderElection GenServer
  """
  def start_link(opts) do
    stream_name = Keyword.fetch!(opts, :kinesis_stream_name)
    Supervisor.start_link(__MODULE__, opts, name: name(stream_name))
  end

  @impl Supervisor
  def init(opts) do
    app_name = Keyword.fetch!(opts, :kinesis_stream_name)
    worker_id = Keyword.fetch!(opts, :worker_id)

    Logger.info("Starting lease supervisor for #{app_name} with worker #{worker_id}")

    children = [
      # Leader election process with restart strategy
      {KinesisClient.HierarchicalShardSyncer.Task, opts},
      {KinesisClient.Stream.LeaseRefresher, opts},
      {KinesisClient.LeaseCoordinator, opts}
    ]

    # One-for-one ensures that if the leader election process crashes,
    # it will be restarted without affecting other processes.
    # We use a max_restarts of 5 in 60 seconds to prevent rapid restart loops.
    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 5,
      max_seconds: 60
    )
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
