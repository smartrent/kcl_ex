defmodule KinesisClient.Stream.AppState do
  @moduledoc """
  The AppState is where the information about Stream shards are stored. ShardConsumers will
  checkpoint the records, and the `KinesisClient.Stream.Coordinator` will check here to determine
  what shards to consume.
  """

  def initialize(app_name, opts \\ []),
    do: adapter(opts).initialize(app_name, opts)

  @doc """
  Get a `KinesisClient.Stream.AppState.ShardInfo` struct by shard_id. If there is not an existing
  record, returns `:not_found`.
  """
  def get_lease(app_name, shard_id, opts \\ []),
    do: adapter(opts).get_lease(app_name, shard_id, opts)

  @doc """
  Persists a new ShardInfo record. Returns an error if there is already a record for that `shard_id`
  """
  def create_lease(app_name, shard_id),
    do: adapter([]).create_lease(app_name, shard_id)

  @doc """
  Update the checkpoint of the shard with the last sequence number that was processed by a
  ShardConsumer. Will return {:error, :lead_invalid} if the `lease` does not match what is in
  `ShardInfo` and the checkpoint will not be updated.
  """
  def update_checkpoint(app_name, shard_id, lease, checkpoint, opts \\ []),
    do: adapter(opts).update_checkpoint(app_name, shard_id, lease, checkpoint, opts)

  @doc """
  Renew lease. Increments :lease_count.
  """
  def renew_lease(app_name, shard_lease),
    do: adapter([]).renew_lease(app_name, shard_lease)

  def take_lease(app_name, shard_id, new_owner, opts \\ [], lease_status \\ "LEASED"),
    do: adapter(opts).take_lease(app_name, shard_id, new_owner, opts, lease_status)

  @doc """
  Marks a ShardLease as completed.

  This indicates that all records for the shard have been processed by the app. `KinesisClient.Stream.Shard`
  processes will not be started for ShardLease's that are completed.
  """
  def close_shard(app_name, shard_id, lease_owner),
    do: adapter([]).close_shard(app_name, shard_id, lease_owner)

  @doc """
  Lists all leases in the app's lease table.
  """
  def list_all_leases(app_name),
    do: adapter([]).list_all_leases(app_name)

  @doc """
  Lists all leases owned by a specific worker.
  """
  def list_worker_leases(app_name, lease_owner),
    do: adapter([]).list_worker_leases(app_name, lease_owner)

  @doc """
  Lists all available leases (not assigned to any worker).
  """
  def list_available_leases(app_name),
    do: adapter([]).list_available_leases(app_name)

  @doc """
  Lists all completed leases (shards that are fully processed).
  """
  def list_completed_leases(app_name),
    do: adapter([]).list_completed_leases(app_name)

  @doc """
  Lists all active leases (assigned to workers and not completed).
  """
  def list_active_leases(app_name),
    do: adapter([]).list_active_leases(app_name)

  @doc """
  Makes a lease available for other workers to claim.
  Typically used during worker cleanup to release leases from workers that are no longer active.
  """

  defp adapter(opts) do
    Keyword.get(opts, :adapter, KinesisClient.Stream.AppState.Dynamo)
  end
end
