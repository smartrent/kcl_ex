# KCL Production Alerting Configuration

This document outlines the critical alerts you should configure for your KCL 3.0 Elixir application in production.

## Critical Alerts (Page-worthy)

### 🚨 Data Processing Failures

**Alert: High Message Processing Failure Rate**
```
kinesis_client_shard_processing_ack_failure_total 
/ (kinesis_client_shard_processing_ack_success_total + kinesis_client_shard_processing_ack_failure_total) > 0.05
```
- **Threshold**: >5% failure rate over 5 minutes
- **Severity**: Critical
- **Action**: Page on-call engineer

**Alert: No Messages Processed**
```
rate(kinesis_client_shard_processing_ack_success_total[10m]) == 0
```
- **Threshold**: No successful messages in 10 minutes (during business hours)
- **Severity**: Critical
- **Action**: Page on-call engineer

### 🚨 Stream Lag Issues

**Alert: Consumer Lag Too High**
```
kinesis_client_shard_millis_behind_latest > 300000
```
- **Threshold**: >5 minutes behind latest
- **Severity**: Critical
- **Action**: Page on-call engineer immediately

**Alert: Consumer Lag Growing**
```
increase(kinesis_client_shard_millis_behind_latest[15m]) > 60000
```
- **Threshold**: Lag increasing by >1 minute over 15 minutes
- **Severity**: Warning → Critical if sustained
- **Action**: Alert team, page if trend continues

### 🚨 Lease Management Failures

**Alert: High Lease Renewal Failure Rate**
```
kinesis_client_lease_renewal_failure_total 
/ (kinesis_client_lease_renewal_success_total + kinesis_client_lease_renewal_failure_total) > 0.1
```
- **Threshold**: >10% lease renewal failures over 5 minutes
- **Severity**: Critical
- **Action**: Page on-call engineer

**Alert: No Active Workers**
```
kinesis_client_workers_active_count == 0
```
- **Threshold**: No active workers for 2 minutes
- **Severity**: Critical
- **Action**: Page on-call engineer immediately

### 🚨 Leadership Issues

**Alert: Frequent Leadership Changes**
```
rate(kinesis_client_leadership_election_total[30m]) > 6
```
- **Threshold**: >6 leadership elections in 30 minutes
- **Severity**: Warning → Critical if sustained
- **Action**: Investigation required

**Alert: No Leader for Extended Period**
```
absent_over_time(kinesis_client_leadership_heartbeat_total{status="success"}[10m])
```
- **Threshold**: No successful leadership heartbeat for 10 minutes
- **Severity**: Critical
- **Action**: Page on-call engineer

## Warning Alerts

### ⚠️ Performance Degradation

**Alert: High Processing Latency**
```
histogram_quantile(0.95, kinesis_client_shard_processing_duration_ms) > 30000
```
- **Threshold**: 95th percentile processing time >30 seconds
- **Severity**: Warning
- **Action**: Investigation during business hours

**Alert: High DynamoDB Latency**
```
histogram_quantile(0.95, kinesis_client_dynamodb_operation_duration_ms) > 1000
```
- **Threshold**: 95th percentile DynamoDB operation >1 second
- **Severity**: Warning
- **Action**: Check DynamoDB performance

### ⚠️ Resource Utilization

**Alert: High Memory Usage**
```
kinesis_client_system_memory_usage_bytes{type="total"} / (1024*1024*1024) > 2
```
- **Threshold**: >2GB total memory usage per worker
- **Severity**: Warning
- **Action**: Monitor for memory leaks

**Alert: High Process Count**
```
kinesis_client_system_process_count > 100000
```
- **Threshold**: >100k processes in BEAM VM
- **Severity**: Warning
- **Action**: Check for process leaks

**Alert: High Message Queue Lengths**
```
kinesis_client_system_message_queue_max_length > 1000
```
- **Threshold**: Any process with >1000 queued messages
- **Severity**: Warning
- **Action**: Check for bottlenecks

### ⚠️ API Issues

**Alert: DynamoDB Throttling**
```
rate(kinesis_client_dynamodb_throttled_total[5m]) > 0
```
- **Threshold**: Any DynamoDB throttling over 5 minutes
- **Severity**: Warning
- **Action**: Consider increasing DynamoDB capacity

**Alert: Kinesis API Throttling**
```
rate(kinesis_client_kinesis_throttled_total[5m]) > 0
```
- **Threshold**: Any Kinesis throttling over 5 minutes
- **Severity**: Warning
- **Action**: Review shard configuration

## Informational Alerts

### 📊 Operational Metrics

**Alert: Worker Count Change**
```
changes(kinesis_client_workers_active_count[1h]) > 5
```
- **Threshold**: >5 worker count changes in 1 hour
- **Severity**: Info
- **Action**: Log for capacity planning

**Alert: Shard Rebalancing**
```
rate(kinesis_client_rebalancing_events_total[1h]) > 2
```
- **Threshold**: >2 rebalancing events per hour
- **Severity**: Info
- **Action**: Monitor for stability

## Recommended Dashboards

### Primary Dashboard
- Stream lag by shard (time series)
- Processing throughput (messages/sec)
- Error rates by type
- Active worker count
- Leadership status

### Secondary Dashboard
- DynamoDB operation latency and errors
- Kinesis API call patterns
- System resource utilization
- Lease renewal success rates

### Debug Dashboard
- Individual shard performance
- Worker-specific metrics
- Detailed error breakdown
- GC and memory patterns

## SLA Recommendations

Based on typical KCL deployments:

### Availability SLAs
- **Stream Processing Uptime**: 99.9% (excluding planned maintenance)
- **Maximum Lag**: <5 minutes during normal operations
- **Recovery Time**: <15 minutes for automatic recovery

### Performance SLAs
- **Processing Latency**: 95% of batches processed within 30 seconds
- **Lease Renewal Success**: >99% success rate
- **Leadership Stability**: <2 leadership changes per hour

## Runbook Links

For each critical alert, ensure you have runbooks covering:

1. **Stream Lag Issues**
   - Check Kinesis stream health
   - Review shard count vs. worker count
   - Investigate downstream processing bottlenecks

2. **Lease Management Failures**
   - Check DynamoDB table health and capacity
   - Review worker registry consistency
   - Verify IAM permissions

3. **Worker Health Issues**
   - Check deployment status
   - Review system resources (CPU, memory)
   - Verify network connectivity to AWS services

4. **Leadership Election Problems**
   - Check DynamoDB leader table
   - Review worker registry for stale entries
   - Verify time synchronization across workers

## Testing Your Alerts

Create test scenarios to validate your alerting:

1. **Simulate high lag**: Temporarily stop workers while stream receives data
2. **Simulate failures**: Introduce network issues to DynamoDB
3. **Simulate worker failure**: Kill worker processes to test failover
4. **Load testing**: Generate high throughput to test thresholds

Remember to test both the alert triggers and the recovery procedures! 