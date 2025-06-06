defmodule KinesisClient.Leadership.AppState do
  def put_leader_item(table_name, worker_id, current_time, opts) do
    adapter(opts).put_leader_item(table_name, worker_id, current_time, opts)
  end

  def update_leader_heartbeat(table_name, worker_id, current_time, opts) do
    adapter(opts).update_leader_heartbeat(table_name, worker_id, current_time, opts)
  end

  def get_leader_info(table_name, opts) do
    adapter(opts).get_leader_info(table_name, opts)
  end

  def describe_table(table_name, opts) do
    adapter(opts).describe_table(table_name, opts)
  end

  def create_table(table_name, hash_key, key_schema, read_capacity, write_capacity, opts) do
    adapter(opts).create_table(
      table_name,
      hash_key,
      key_schema,
      read_capacity,
      write_capacity,
      opts
    )
  end

  def delete_leader_item(table_name, worker_id, opts) do
    adapter(opts).delete_leader_item(table_name, worker_id, opts)
  end

  defp adapter(opts) do
    Keyword.get(opts, :adapter, KinesisClient.Leadership.AppState.Dynamo)
  end
end
