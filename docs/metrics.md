# KinesisClient Telemetry Metrics Reference

This document provides a comprehensive reference for all telemetry metrics available in the KinesisClient library. These metrics are essential for monitoring, alerting, and troubleshooting your Kinesis stream processing applications.

## 📊 Metric Categories

- [Shard Processing Metrics](#shard-processing-metrics)
- [Lease Management Metrics](#lease-management-metrics)
- [Leadership Election Metrics](#leadership-election-metrics)
- [Worker Registry Metrics](#worker-registry-metrics)
- [DynamoDB Operation Metrics](#dynamodb-operation-metrics)
- [Kinesis API Metrics](#kinesis-api-metrics)
- [System Health Metrics](#system-health-metrics)
- [Rebalancing Metrics](#rebalancing-metrics)

---

## Shard Processing Metrics

These metrics track the core data processing functionality of your KCL application.

### `kinesis_client_shard_processing_records_total`
**Type:** Counter  
**Description:** Total number of records processed from Kinesis shards  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`, `host`  
**Use Case:** Monitor overall throughput and processing volume

```promql
# Rate of records processed per second
rate(kinesis_client_shard_processing_records_total[5m])

# Records processed by shard
sum by (shard_id) (kinesis_client_shard_processing_records_total)
```

### `kinesis_client_shard_processing_ack_success_total`
**Type:** Counter  
**Description:** Total number of successfully acknowledged messages  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`, `host`  
**Use Case:** Track successful message processing

```promql
# Success rate over time
rate(kinesis_client_shard_processing_ack_success_total[5m])
```

### `kinesis_client_shard_processing_ack_failure_total`
**Type:** Counter  
**Description:** Total number of failed message acknowledgments  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`, `host`  
**Use Case:** Monitor processing failures for alerting

```promql
# Failure rate
rate(kinesis_client_shard_processing_ack_failure_total[5m])

# Error percentage
rate(kinesis_client_shard_processing_ack_failure_total[5m]) / 
(rate(kinesis_client_shard_processing_ack_success_total[5m]) + 
 rate(kinesis_client_shard_processing_ack_failure_total[5m])) * 100
```

### `kinesis_client_shard_processing_batch_size`
**Type:** Distribution  
**Description:** Distribution of batch sizes processed  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`  
**Use Case:** Optimize batch sizes and understand processing patterns

```promql
# Average batch size
histogram_quantile(0.5, kinesis_client_shard_processing_batch_size)

# 95th percentile batch size
histogram_quantile(0.95, kinesis_client_shard_processing_batch_size)
```

### `kinesis_client_shard_processing_duration_ms`
**Type:** Distribution  
**Description:** Time taken to process message batches (milliseconds)  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`  
**Use Case:** Monitor processing latency and performance

```promql
# Average processing time
histogram_quantile(0.5, kinesis_client_shard_processing_duration_ms)

# Slow processing alert (>30 seconds)
histogram_quantile(0.95, kinesis_client_shard_processing_duration_ms) > 30000
```

### `kinesis_client_shard_millis_behind_latest`
**Type:** Last Value  
**Description:** Milliseconds behind the latest record in the stream  
**Labels:** `app_name`, `stream_name`, `shard_id`, `host`  
**Use Case:** Monitor consumer lag - critical for real-time processing

```promql
# Current lag per shard
kinesis_client_shard_millis_behind_latest

# High lag alert (>5 minutes)
kinesis_client_shard_millis_behind_latest > 300000
```

### `kinesis_client_shard_lifecycle_start_total`
**Type:** Counter  
**Description:** Total number of shard processing starts  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`  
**Use Case:** Track shard lifecycle events and restarts

### `kinesis_client_shard_lifecycle_stop_total`
**Type:** Counter  
**Description:** Total number of shard processing stops  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`, `reason`  
**Use Case:** Monitor shard shutdowns and reasons

### `kinesis_client_shard_errors_total`
**Type:** Counter  
**Description:** Total number of shard processing errors  
**Labels:** `app_name`, `stream_name`, `shard_id`, `worker_id`, `error_type`  
**Use Case:** Track and categorize processing errors

---

## Lease Management Metrics

These metrics monitor the lease management system that coordinates shard ownership.

### `kinesis_client_lease_operations_total`
**Type:** Counter  
**Description:** Total lease operations (take, renew, release)  
**Labels:** `app_name`, `worker_id`, `operation`, `status`  
**Use Case:** Monitor lease system health

```promql
# Lease operation rate by type
rate(kinesis_client_lease_operations_total[5m])

# Failed lease operations
kinesis_client_lease_operations_total{status="failure"}
```

### `kinesis_client_lease_operation_duration_ms`
**Type:** Distribution  
**Description:** Duration of lease operations (milliseconds)  
**Labels:** `app_name`, `worker_id`, `operation`  
**Use Case:** Monitor lease operation performance

```promql
# Slow lease operations (>1 second)
histogram_quantile(0.95, kinesis_client_lease_operation_duration_ms) > 1000
```

### `kinesis_client_lease_count`
**Type:** Last Value  
**Description:** Current number of leases by status  
**Labels:** `app_name`, `worker_id`, `status`  
**Use Case:** Monitor lease distribution across workers

```promql
# Total active leases
sum(kinesis_client_lease_count{status="active"})

# Leases per worker
kinesis_client_lease_count by (worker_id)
```

### `kinesis_client_lease_renewal_success_total`
**Type:** Counter  
**Description:** Total successful lease renewals  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Monitor lease renewal health

### `kinesis_client_lease_renewal_failure_total`
**Type:** Counter  
**Description:** Total failed lease renewals  
**Labels:** `app_name`, `worker_id`, `reason`  
**Use Case:** Alert on lease renewal problems

```promql
# Lease renewal failure rate
rate(kinesis_client_lease_renewal_failure_total[5m]) /
rate(kinesis_client_lease_renewal_success_total[5m]) * 100
```

### `kinesis_client_lease_renewal_interval_ms`
**Type:** Distribution  
**Description:** Time between lease renewals (milliseconds)  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Monitor lease renewal timing

---

## Leadership Election Metrics

Track the leader election system that coordinates cluster-wide operations.

### `kinesis_client_leadership_election_total`
**Type:** Counter  
**Description:** Total leadership election attempts  
**Labels:** `app_name`, `worker_id`, `result`  
**Use Case:** Monitor leadership stability

```promql
# Leadership election rate
rate(kinesis_client_leadership_election_total[30m])

# Failed elections
kinesis_client_leadership_election_total{result="failed"}
```

### `kinesis_client_leadership_tenure_ms`
**Type:** Distribution  
**Description:** Duration of leadership tenure (milliseconds)  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Analyze leadership stability

### `kinesis_client_leadership_heartbeat_total`
**Type:** Counter  
**Description:** Total leadership heartbeat attempts  
**Labels:** `app_name`, `worker_id`, `status`  
**Use Case:** Monitor leader health

### `kinesis_client_leadership_abandonment_total`
**Type:** Counter  
**Description:** Total leadership abandonments  
**Labels:** `app_name`, `worker_id`, `reason`  
**Use Case:** Track leadership failures

---

## Worker Registry Metrics

Monitor the worker registry that tracks active cluster members.

### `kinesis_client_workers_active_count`
**Type:** Last Value  
**Description:** Current number of active workers in the cluster  
**Labels:** `app_name`  
**Use Case:** Monitor cluster size and availability

```promql
# Alert when no workers active
kinesis_client_workers_active_count == 0

# Worker count changes
changes(kinesis_client_workers_active_count[1h])
```

### `kinesis_client_workers_registration_total`
**Type:** Counter  
**Description:** Total worker registration attempts  
**Labels:** `app_name`, `worker_id`, `status`  
**Use Case:** Track worker registration health

### `kinesis_client_workers_heartbeat_total`
**Type:** Counter  
**Description:** Total worker heartbeat attempts  
**Labels:** `app_name`, `worker_id`, `status`  
**Use Case:** Monitor worker health

### `kinesis_client_workers_cleanup_total`
**Type:** Counter  
**Description:** Total stale worker cleanup operations  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Track worker cleanup activity

---

## DynamoDB Operation Metrics

Monitor all DynamoDB operations used by the lease and registry systems.

### `kinesis_client_dynamodb_operations_total`
**Type:** Counter  
**Description:** Total DynamoDB operations  
**Labels:** `app_name`, `table_name`, `operation`, `status`  
**Use Case:** Monitor DynamoDB usage and errors

```promql
# DynamoDB operation rate
rate(kinesis_client_dynamodb_operations_total[5m])

# Failed operations
kinesis_client_dynamodb_operations_total{status="failure"}
```

### `kinesis_client_dynamodb_operation_duration_ms`
**Type:** Distribution  
**Description:** Duration of DynamoDB operations (milliseconds)  
**Labels:** `app_name`, `table_name`, `operation`  
**Use Case:** Monitor DynamoDB performance

```promql
# Slow DynamoDB operations
histogram_quantile(0.95, kinesis_client_dynamodb_operation_duration_ms) > 1000
```

### `kinesis_client_dynamodb_throttled_total`
**Type:** Counter  
**Description:** Total DynamoDB throttled requests  
**Labels:** `app_name`, `table_name`, `operation`  
**Use Case:** Monitor capacity issues

### `kinesis_client_dynamodb_errors_total`
**Type:** Counter  
**Description:** Total DynamoDB operation errors  
**Labels:** `app_name`, `table_name`, `operation`, `error_type`  
**Use Case:** Track DynamoDB error patterns

---

## Kinesis API Metrics

Monitor interactions with the Kinesis service.

### `kinesis_client_kinesis_get_records_total`
**Type:** Counter  
**Description:** Total Kinesis GetRecords API calls  
**Labels:** `app_name`, `stream_name`, `shard_id`, `status`  
**Use Case:** Monitor Kinesis API usage

### `kinesis_client_kinesis_get_records_duration_ms`
**Type:** Distribution  
**Description:** Duration of Kinesis GetRecords calls (milliseconds)  
**Labels:** `app_name`, `stream_name`, `shard_id`  
**Use Case:** Monitor Kinesis API performance

### `kinesis_client_kinesis_throttled_total`
**Type:** Counter  
**Description:** Total Kinesis API throttled requests  
**Labels:** `app_name`, `stream_name`, `shard_id`  
**Use Case:** Monitor Kinesis throttling

### `kinesis_client_kinesis_records_per_call`
**Type:** Distribution  
**Description:** Number of records returned per GetRecords call  
**Labels:** `app_name`, `stream_name`, `shard_id`  
**Use Case:** Optimize GetRecords batch sizes

---

## System Health Metrics

Monitor the health of the BEAM VM and system resources.

### `kinesis_client_system_memory_usage_bytes`
**Type:** Last Value  
**Description:** Memory usage in bytes by type  
**Labels:** `app_name`, `worker_id`, `type`  
**Types:** `total`, `processes`, `system`, `atom`, `binary`, `code`, `ets`  
**Use Case:** Monitor memory consumption

```promql
# Total memory usage
kinesis_client_system_memory_usage_bytes{type="total"}

# Memory usage by type
kinesis_client_system_memory_usage_bytes by (type)
```

### `kinesis_client_system_process_count`
**Type:** Last Value  
**Description:** Current number of processes in the BEAM VM  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Monitor for process leaks

### `kinesis_client_system_gc_duration_ms`
**Type:** Distribution  
**Description:** Garbage collection duration (milliseconds)  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Monitor GC performance impact

---

## Rebalancing Metrics

Track lease rebalancing operations that redistribute work across workers.

### `kinesis_client_rebalancing_events_total`
**Type:** Counter  
**Description:** Total rebalancing events  
**Labels:** `app_name`, `worker_id`, `event_type`  
**Event Types:** `trigger`, `complete`, `fail`  
**Use Case:** Monitor rebalancing frequency

### `kinesis_client_rebalancing_duration_ms`
**Type:** Distribution  
**Description:** Duration of rebalancing operations (milliseconds)  
**Labels:** `app_name`, `worker_id`  
**Use Case:** Monitor rebalancing performance

### `kinesis_client_rebalancing_lease_moves`
**Type:** Distribution  
**Description:** Number of leases moved during rebalancing  
**Labels:** `app_name`  
**Use Case:** Understand rebalancing impact

---

## 🔥 Key Metrics for Alerting

### Critical Alerts
1. **Stream Lag**: `kinesis_client_shard_millis_behind_latest > 300000` (>5 minutes)
2. **Processing Failures**: High failure rate in `kinesis_client_shard_processing_ack_failure_total`
3. **No Workers**: `kinesis_client_workers_active_count == 0`
4. **Lease Failures**: High rate in `kinesis_client_lease_renewal_failure_total`

### Warning Alerts
1. **High Latency**: `histogram_quantile(0.95, kinesis_client_shard_processing_duration_ms) > 30000`
2. **DynamoDB Issues**: `kinesis_client_dynamodb_throttled_total` or high error rates
3. **Memory Usage**: `kinesis_client_system_memory_usage_bytes{type="total"}` growth
4. **Frequent Rebalancing**: High rate in `kinesis_client_rebalancing_events_total`

## 📈 Common Queries

### Throughput Analysis
```promql
# Records per second by shard
rate(kinesis_client_shard_processing_records_total[5m]) by (shard_id)

# Total application throughput
sum(rate(kinesis_client_shard_processing_records_total[5m]))
```

### Error Analysis
```promql
# Error rate percentage
rate(kinesis_client_shard_processing_ack_failure_total[5m]) / 
(rate(kinesis_client_shard_processing_ack_success_total[5m]) + 
 rate(kinesis_client_shard_processing_ack_failure_total[5m])) * 100

# Top error types
topk(5, sum by (error_type) (kinesis_client_shard_errors_total))
```

### Performance Analysis
```promql
# Processing latency percentiles
histogram_quantile(0.50, kinesis_client_shard_processing_duration_ms) # Median
histogram_quantile(0.95, kinesis_client_shard_processing_duration_ms) # 95th percentile
histogram_quantile(0.99, kinesis_client_shard_processing_duration_ms) # 99th percentile
```

### System Health
```promql
# Memory usage trend
kinesis_client_system_memory_usage_bytes{type="total"}

# Worker stability
changes(kinesis_client_workers_active_count[1h])
```

---

This comprehensive metrics reference helps you build effective monitoring, alerting, and troubleshooting strategies for your KinesisClient applications. 