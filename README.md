# KCL

Implements a native Elixir implementation of Amazon's Kinesis Client Library
(KCL). The KCL is a Java library that uses a DynamoDB table to keep track of
how far an app has processed a Kinesis stream and to correctly handle shard
splits and merges.

By using this library, you get the above functionality without the need to
deploy a the KCL Multilang Daemon.


## Install
Add this to your dependencies
```
    {:kinesis_client, "~> 0.1.0"},
```
and run `mix deps.get`

## Usage

Stream processing uses Broadway pipelines:

```elixir
opts = [
  stream_name: "kcl-ex-test-stream",
  app_name: "my-test-app",
  shard_consumer: MyShardConsumer,
  processors: [default: [concurrency: 1, min_demand: 10, max_demand: 20]],
  batchers: [default: [concurrency: 1, batch_size: 40]]
]

KinesisClient.Stream.start_link(opts)
```

`MyShardConsumer` implements the `Broadway` behaviour. Add to your supervision tree.

## Configuration

All timer intervals and behaviors are configurable with sensible defaults:

```elixir
config :kinesis_client,
  # Leader Election (ms)
  leader_lease_duration_ms: 30_000,
  leader_heartbeat_interval_ms: 3_000,
  leader_takeover_grace_period_ms: 5_004,
  leader_max_consecutive_failures: 3,

  # Worker Management (ms)
  worker_heartbeat_interval_ms: 5_000,
  worker_cleanup_interval_ms: 30_000,
  worker_timeout_ms: 60_000,

  # Lease Operations (ms)
  lease_take_interval_ms: 20_000,
  lease_renew_interval_ms: 10_000,
  lease_assignment_interval_ms: 60_000,
  lease_check_interval_ms: 5_000,

  # System Monitoring (ms)
  shard_poll_interval_ms: 5_000,
  system_health_check_interval_ms: 30_000,
  shard_sync_interval_ms: 60_000
```

### Environment-Specific Tuning

```elixir
# Development - faster feedback
config :kinesis_client,
  leader_heartbeat_interval_ms: 1_000,
  lease_renew_interval_ms: 5_000

# Production - stability focused  
config :kinesis_client,
  leader_lease_duration_ms: 60_000,
  worker_timeout_ms: 120_000

# High-throughput - performance optimized
config :kinesis_client,
  shard_poll_interval_ms: 1_000,
  system_health_check_interval_ms: 15_000
```

### Telemetry

```elixir
config :kinesis_client, :telemetry,
  console_enabled: true,        # Development
  prometheus_enabled: false,    # Production monitoring
  prometheus_port: 9090
```

Configuration validation happens at startup and is passed down through the supervision tree.

## Important Notes

Keep processor and batch concurrency at `1` for guaranteed message processing order. Kinesis uses checkpointing rather than individual message acknowledgments. Scale throughput by increasing shard count or fan-out processing in your `handle_batch/4` callback.

## Troubleshooting

**High latency**: Reduce `shard_poll_interval_ms` and `lease_renew_interval_ms`  
**High CPU usage**: Increase heartbeat and health check intervals  
**Lease conflicts**: Increase `leader_lease_duration_ms` and grace periods  
**Stale workers**: Reduce `worker_timeout_ms`, increase `worker_cleanup_interval_ms`  

Debug with `config :logger, level: :debug`.

## Development

Tests require [localstack](https://github.com/localstack/localstack):
```bash
SERVICES=kinesis,dynamodb localstack start --host
# or
docker compose up -d && AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy mix test
```