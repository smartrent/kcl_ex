defmodule KinesisClient.HierarchicalShardSyncer.Task do
  @moduledoc """
  Supervisor for the leader election components in KinesisClient.

  This supervisor ensures that the leader election process stays alive
  and properly restarts if it fails. This follows KCL 3.x's approach to
  leader election management.
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
    state = %{
      kinesis_stream_name: opts[:kinesis_stream_name],
      dynamo_table_name: opts[:dynamo_table_name]
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

    Process.send_after(self(), :sync_shards, 5_000)
    {:noreply, state}
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
