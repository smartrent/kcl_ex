defmodule KinesisClient.Telemetry do
  @moduledoc """
  Defines telemetry metrics for the KinesisClient application.
  Used for local monitoring and debugging.
  """

  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      {Telemetry.Metrics.ConsoleReporter,
       metrics: [
         counter("kinesis_client.shard.processing.ack.success.count",
           tags: [:shard_id, :worker_id, :host],
           description: "Number of successfully acknowledged messages"
         ),
         counter("kinesis_client.shard.processing.start.count",
           tags: [:shard_id, :worker_id],
           description: "Number of times shard processing started"
         ),
         counter("kinesis_client.shard.get_records.millis_behind_latest",
           tags: [:shard_id, :host],
           description: "Counter for milliseconds behind latest for shard records"
         )
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
