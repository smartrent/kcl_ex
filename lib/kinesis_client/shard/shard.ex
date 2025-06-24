defmodule KinesisClient.Stream.Shard do
  @moduledoc false
  use Supervisor, restart: :transient
  alias KinesisClient.Stream.Shard.{Lease, Pipeline}
  require Logger
  import KinesisClient.Util

  def start_link(args) do
    Logger.info("Starting shard #{args[:shard_id]} for stream #{args[:stream_name]}")
    Supervisor.start_link(__MODULE__, args, name: args[:shard_name])
  end

  def init(opts) do
    lease_opts = [
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      config: opts[:config]
    ]

    pipeline_opts = [
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      stream_name: opts[:stream_name],
      shard_consumer: opts[:shard_consumer],
      processors: opts[:processors],
      batchers: opts[:batchers],
      coordinator_name: opts[:coordinator_name]
    ]

    lease_opts =
      lease_opts
      |> optional_kw(:app_state_opts, Keyword.get(opts, :app_state_opts))
      |> optional_kw(:renew_interval, Keyword.get(opts, :lease_renew_interval))
      |> optional_kw(:lease_expiry, Keyword.get(opts, :lease_expiry))

    children = [
      %{
        id: Pipeline,
        start: {Pipeline, :start_link, [pipeline_opts]},
        restart: :temporary,
        shutdown: 5000
      },
      %{
        id: Lease,
        start: {Lease, :start_link, [lease_opts]},
        restart: :temporary,
        shutdown: 5000
      }
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def start(supervisor, shard_info) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, shard_info})
  end

  def stop(shard_name) do
    Logger.info("Stopping shard: #{inspect(shard_name)}")

    shard_pid =
      case shard_name do
        pid when is_pid(pid) -> pid
        name -> Process.whereis(name)
      end

    Supervisor.stop(shard_pid, :shutdown, 30_000)

    :ok
  end

  def name(stream_name, shard_id) do
    result = Module.concat(__MODULE__, stream_name)
    Module.concat(result, shard_id)
  end
end
