defmodule KinesisClient.HierarchicalShardSyncer.Task do
  @moduledoc """
  GenServer task for hierarchical shard synchronization.

  Periodically synchronizes shard hierarchy from Kinesis stream
  following KCL 3.x approach.
  """
  use GenServer
  require Logger

  @doc """
  Starts the leader election supervisor.

  ## Options
    * `:app_name` - Required. The name of the application
    * `:worker_id` - Required. Unique identifier for this worker
    * All other options are passed to the LeaderElection GenServer
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(opts[:app_name]))
  end

  @impl GenServer
  def init(opts) do
    config = Keyword.fetch!(opts, :config)

    state = %{
      kinesis_stream_name: opts[:kinesis_stream_name],
      dynamo_table_name: opts[:dynamo_table_name],
      config: config
    }

    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    Process.send_after(self(), :sync_shards, 1_000)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:sync_shards, state) do
    KinesisClient.HierarchicalShardSyncer.sync_hierarchy(
      state.kinesis_stream_name,
      state.dynamo_table_name
    )

    Process.send_after(self(), :sync_shards, state.config[:shard_sync_interval_ms])
    {:noreply, state}
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
