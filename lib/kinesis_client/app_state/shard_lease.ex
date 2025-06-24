defmodule KinesisClient.Stream.AppState.ShardLease do
  @moduledoc """
  Stores information about the shard including its checkpoint value
  """
  @derive ExAws.Dynamo.Encodable

  defstruct [
    # Unique identifier for the shard
    :shard_id,
    # Latest processed sequence number
    :checkpoint,
    # Current worker owning the lease
    :lease_owner,
    # Counter for optimistic locking
    :lease_count,
    # ID of the parent shard (if any)
    :parent_shard_id,
    # Whether parent shard is completed
    :parent_shard_completed,
    # Whether processing of this shard is complete
    :completed,
    # Last time the lease was renewed
    :last_renewal_time,
    # Total bytes processed through this shard
    :throughput_bytes,
    # Bytes processed in last reporting interval
    :last_throughput_bytes,
    # Duration of last throughput measurement
    :last_throughput_millis,
    # Exponential moving average of throughput (bytes/sec)
    :average_throughput,
    # Total number of records processed
    :records_processed,
    # When processing of this shard began
    :processing_started_at,
    # Last time processing stats were updated
    :last_processing_stats_update,
    :parent_shard_ids,
    # Values can be: "AVAILABLE", "LEASED", "COMPLETED"
    lease_status: "AVAILABLE"
  ]
end
