defmodule KinesisClient.Telemetry do
  @moduledoc """
  Defines telemetry metrics for the KinesisClient application.
  Used for production monitoring, alerting, and observability.

  ## Configuration

  The telemetry system can be configured in your application config:

      config :kinesis_client, :telemetry,
        console_enabled: true,           # Enable console reporter (default: true)
        prometheus_enabled: false,       # Enable Prometheus reporter (default: false)
        prometheus_port: 9090,           # Prometheus metrics port (default: 9090)
        console_device: :stdio,          # Console output device (default: :stdio)
        console_format: :default         # Console output format (default: :default)

  ## Console Reporter

  The ConsoleReporter prints telemetry events to the terminal, which is useful for:
  - Development and debugging
  - Discovering available measurements and metadata
  - Monitoring system behavior in real-time

  Example output:
      [Telemetry.Metrics.ConsoleReporter] kinesis_client.shard.processing.ack.success.total: 1
      [Telemetry.Metrics.ConsoleReporter] kinesis_client.system.memory.usage_bytes: 45234176 (type: "total")

  ## Prometheus Reporter

  The Prometheus reporter exposes metrics in Prometheus format on a configurable port.
  Enable it in production to integrate with monitoring systems like Grafana.

  ## Usage

  Start the telemetry supervisor as part of your application's supervision tree:

      children = [
        {KinesisClient.Telemetry, []}
      ]

  """

  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    telemetry_config = Application.get_env(:kinesis_client, :telemetry, [])
    backend = Keyword.get(telemetry_config, :backend, :console)
    console_enabled = Keyword.get(telemetry_config, :console_enabled, true)
    prometheus_enabled = Keyword.get(telemetry_config, :prometheus_enabled, false)

    children = []

    children =
      if console_enabled do
        console_opts = build_console_reporter_opts(telemetry_config)
        [{Telemetry.Metrics.ConsoleReporter, console_opts} | children]
      else
        children
      end

    children =
      if prometheus_enabled do
        prometheus_opts = build_prometheus_reporter_opts(telemetry_config)
        [prometheus_opts | children]
      else
        children
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp build_console_reporter_opts(config) do
    base_opts = [metrics: console_metrics()]

    base_opts
    |> maybe_add_opt(:device, Keyword.get(config, :console_device))
    |> maybe_add_opt(:format, Keyword.get(config, :console_format))
  end

  defp build_prometheus_reporter_opts(config) do
    prometheus_name = :"prometheus_metrics_#{:rand.uniform(10000)}"
    port = Keyword.get(config, :prometheus_port, 9090)

    {TelemetryMetricsPrometheus,
     [
       metrics: metrics(),
       name: prometheus_name,
       port: port
     ]}
  end

  defp maybe_add_opt(opts, _key, nil), do: opts
  defp maybe_add_opt(opts, key, value), do: Keyword.put(opts, key, value)

  @doc """
  Returns all production metrics for Prometheus export
  """
  def metrics do
    [
      # === SHARD PROCESSING METRICS ===
      counter("kinesis_client.shard.processing.records.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id, :host],
        description: "Total number of records processed from Kinesis shards"
      ),
      counter("kinesis_client.shard.processing.ack.success.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id, :host],
        description: "Total number of successfully acknowledged messages"
      ),
      counter("kinesis_client.shard.processing.ack.failure.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id, :host],
        description: "Total number of failed message acknowledgments"
      ),
      distribution("kinesis_client.shard.processing.batch_size",
        tags: [:app_name, :stream_name, :shard_id, :worker_id],
        description: "Distribution of batch sizes processed"
      ),
      distribution("kinesis_client.shard.processing.duration_ms",
        tags: [:app_name, :stream_name, :shard_id, :worker_id],
        description: "Time taken to process message batches",
        unit: {:native, :millisecond}
      ),
      last_value("kinesis_client.shard.millis_behind_latest",
        tags: [:app_name, :stream_name, :shard_id, :host],
        description: "Milliseconds behind the latest record in the stream"
      ),
      counter("kinesis_client.shard.lifecycle.start.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id],
        description: "Total number of shard processing starts"
      ),
      counter("kinesis_client.shard.lifecycle.stop.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id, :reason],
        description: "Total number of shard processing stops"
      ),
      counter("kinesis_client.shard.errors.total",
        tags: [:app_name, :stream_name, :shard_id, :worker_id, :error_type],
        description: "Total number of shard processing errors"
      ),

      # === LEASE MANAGEMENT METRICS ===
      counter("kinesis_client.lease.operations.total",
        tags: [:app_name, :worker_id, :operation, :status],
        description: "Total lease operations (take, renew, release)"
      ),
      distribution("kinesis_client.lease.operation.duration_ms",
        tags: [:app_name, :worker_id, :operation],
        description: "Duration of lease operations",
        unit: {:native, :millisecond}
      ),
      last_value("kinesis_client.lease.count",
        tags: [:app_name, :worker_id, :status],
        description: "Current number of leases by status (active, expired, etc.)"
      ),
      counter("kinesis_client.lease.renewal.success.total",
        tags: [:app_name, :worker_id],
        description: "Total successful lease renewals"
      ),
      counter("kinesis_client.lease.renewal.failure.total",
        tags: [:app_name, :worker_id, :reason],
        description: "Total failed lease renewals"
      ),
      distribution("kinesis_client.lease.renewal.interval_ms",
        tags: [:app_name, :worker_id],
        description: "Time between lease renewals",
        unit: {:native, :millisecond}
      ),

      # === LEADERSHIP ELECTION METRICS ===
      counter("kinesis_client.leadership.election.total",
        tags: [:app_name, :worker_id, :result],
        description: "Total leadership election attempts"
      ),
      distribution("kinesis_client.leadership.tenure_ms",
        tags: [:app_name, :worker_id],
        description: "Duration of leadership tenure",
        unit: {:native, :millisecond}
      ),
      counter("kinesis_client.leadership.heartbeat.total",
        tags: [:app_name, :worker_id, :status],
        description: "Total leadership heartbeat attempts"
      ),
      counter("kinesis_client.leadership.abandonment.total",
        tags: [:app_name, :worker_id, :reason],
        description: "Total leadership abandonments"
      ),

      # === WORKER REGISTRY METRICS ===
      last_value("kinesis_client.workers.active.count",
        tags: [:app_name],
        description: "Current number of active workers in the cluster"
      ),
      counter("kinesis_client.workers.registration.total",
        tags: [:app_name, :worker_id, :status],
        description: "Total worker registration attempts"
      ),
      counter("kinesis_client.workers.heartbeat.total",
        tags: [:app_name, :worker_id, :status],
        description: "Total worker heartbeat attempts"
      ),
      counter("kinesis_client.workers.cleanup.total",
        tags: [:app_name, :worker_id],
        description: "Total stale worker cleanup operations"
      ),

      # === DYNAMODB OPERATIONS METRICS ===
      counter("kinesis_client.dynamodb.operations.total",
        tags: [:app_name, :table_name, :operation, :status],
        description: "Total DynamoDB operations"
      ),
      distribution("kinesis_client.dynamodb.operation.duration_ms",
        tags: [:app_name, :table_name, :operation],
        description: "Duration of DynamoDB operations",
        unit: {:native, :millisecond}
      ),
      counter("kinesis_client.dynamodb.throttled.total",
        tags: [:app_name, :table_name, :operation],
        description: "Total DynamoDB throttled requests"
      ),
      counter("kinesis_client.dynamodb.errors.total",
        tags: [:app_name, :table_name, :operation, :error_type],
        description: "Total DynamoDB operation errors"
      ),

      # === KINESIS API METRICS ===
      counter("kinesis_client.kinesis.get_records.total",
        tags: [:app_name, :stream_name, :shard_id, :status],
        description: "Total Kinesis GetRecords API calls"
      ),
      distribution("kinesis_client.kinesis.get_records.duration_ms",
        tags: [:app_name, :stream_name, :shard_id],
        description: "Duration of Kinesis GetRecords calls",
        unit: {:native, :millisecond}
      ),
      counter("kinesis_client.kinesis.throttled.total",
        tags: [:app_name, :stream_name, :shard_id],
        description: "Total Kinesis API throttled requests"
      ),
      distribution("kinesis_client.kinesis.records_per_call",
        tags: [:app_name, :stream_name, :shard_id],
        description: "Number of records returned per GetRecords call"
      ),

      # === SYSTEM HEALTH METRICS ===
      last_value("kinesis_client.system.memory.usage_bytes",
        tags: [:app_name, :worker_id, :type],
        description: "Memory usage in bytes (total, processes, atom, binary, etc.)"
      ),
      last_value("kinesis_client.system.process.count",
        tags: [:app_name, :worker_id],
        description: "Current number of processes in the BEAM VM"
      ),
      distribution("kinesis_client.system.gc.duration_ms",
        tags: [:app_name, :worker_id],
        description: "Garbage collection duration",
        unit: {:native, :millisecond}
      ),

      # === REBALANCING METRICS ===
      counter("kinesis_client.rebalancing.events.total",
        tags: [:app_name, :worker_id, :event_type],
        description: "Total rebalancing events (trigger, complete, fail)"
      ),
      distribution("kinesis_client.rebalancing.duration_ms",
        tags: [:app_name, :worker_id],
        description: "Duration of rebalancing operations",
        unit: {:native, :millisecond}
      ),
      distribution("kinesis_client.rebalancing.lease_moves",
        tags: [:app_name],
        description: "Number of leases moved during rebalancing"
      )
    ]
  end

  @doc """
  Console metrics for development/debugging
  """
  def console_metrics do
    [
      # Shard Processing Metrics
      counter("kinesis_client.shard.processing.ack.success.total",
        tags: [:shard_id, :worker_id, :host],
        description: "Number of successfully acknowledged messages"
      ),
      counter("kinesis_client.shard.processing.start.total",
        tags: [:shard_id, :worker_id],
        description: "Number of times shard processing started"
      ),
      counter("kinesis_client.shard.processing.ack.failure.total",
        tags: [:shard_id, :worker_id, :host],
        description: "Number of failed message acknowledgments"
      ),
      last_value("kinesis_client.shard.millis_behind_latest",
        tags: [:shard_id, :host],
        description: "Milliseconds behind latest for shard records"
      ),

      # System Health Metrics
      last_value("kinesis_client.system.memory.usage_bytes",
        tags: [:worker_id, :type],
        description: "Memory usage in bytes by type"
      ),
      last_value("kinesis_client.system.process.count",
        tags: [:worker_id],
        description: "Current number of processes in the BEAM VM"
      ),
      last_value("kinesis_client.system.message_queue.max_length",
        tags: [:worker_id],
        description: "Maximum message queue length across all processes"
      ),
      last_value("kinesis_client.system.message_queue.avg_length",
        tags: [:worker_id],
        description: "Average message queue length across all processes"
      ),
      counter("kinesis_client.system.gc.count",
        tags: [:worker_id],
        description: "Number of garbage collection runs since last check"
      ),
      counter("kinesis_client.system.gc.words_reclaimed",
        tags: [:worker_id],
        description: "Words reclaimed by garbage collection since last check"
      ),
      last_value("kinesis_client.system.scheduler.avg_utilization",
        tags: [:worker_id],
        description: "Average scheduler utilization percentage"
      ),
      last_value("kinesis_client.system.scheduler.scheduler_count",
        tags: [:worker_id],
        description: "Number of schedulers available"
      ),

      # Worker Health Metrics
      last_value("kinesis_client.worker.health.is_registered",
        tags: [:worker_id],
        description: "Whether this worker is registered (1) or not (0)"
      ),
      last_value("kinesis_client.worker.health.active_count",
        tags: [:worker_id],
        description: "Total number of active workers in the cluster"
      ),

      # Lease Management Metrics
      counter("kinesis_client.lease.renewal.batch.success_count",
        tags: [:worker_id],
        description: "Number of successful lease renewals in batch"
      ),
      counter("kinesis_client.lease.renewal.batch.failure_count",
        tags: [:worker_id],
        description: "Number of failed lease renewals in batch"
      ),
      distribution("kinesis_client.lease.renewal.batch.duration_ms",
        tags: [:worker_id],
        description: "Duration of lease renewal batch operations",
        unit: {:native, :millisecond}
      ),
      counter("kinesis_client.lease.renewal.failure.count",
        tags: [:worker_id, :shard_id, :reason],
        description: "Individual lease renewal failures"
      )
    ]
  end

  @doc """
  Emit telemetry event for shard processing performance
  """
  def emit_shard_processing_duration(
        app_name,
        stream_name,
        shard_id,
        worker_id,
        duration_ms,
        record_count
      ) do
    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :duration],
      %{duration_ms: duration_ms, record_count: record_count},
      %{app_name: app_name, stream_name: stream_name, shard_id: shard_id, worker_id: worker_id}
    )
  end

  @doc """
  Emit telemetry event for lease operations
  """
  def emit_lease_operation(app_name, worker_id, operation, status, duration_ms \\ nil) do
    measurements = %{count: 1}

    measurements =
      if duration_ms, do: Map.put(measurements, :duration_ms, duration_ms), else: measurements

    :telemetry.execute(
      [:kinesis_client, :lease, :operation],
      measurements,
      %{app_name: app_name, worker_id: worker_id, operation: operation, status: status}
    )
  end

  @doc """
  Emit telemetry event for DynamoDB operations
  """
  def emit_dynamodb_operation(app_name, table_name, operation, status, duration_ms) do
    :telemetry.execute(
      [:kinesis_client, :dynamodb, :operation],
      %{duration_ms: duration_ms, count: 1},
      %{app_name: app_name, table_name: table_name, operation: operation, status: status}
    )
  end

  @doc """
  Emit telemetry event for system health metrics
  """
  def emit_system_health() do
    memory_info = :erlang.memory()
    process_count = :erlang.system_info(:process_count)

    Enum.each(memory_info, fn {type, bytes} ->
      :telemetry.execute(
        [:kinesis_client, :system, :memory],
        %{usage_bytes: bytes},
        %{type: Atom.to_string(type), worker_id: Node.self()}
      )
    end)

    :telemetry.execute(
      [:kinesis_client, :system, :process],
      %{count: process_count},
      %{worker_id: Node.self()}
    )
  end
end
