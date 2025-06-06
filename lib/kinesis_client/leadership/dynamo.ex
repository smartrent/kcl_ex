defmodule KinesisClient.Leadership.AppState.Dynamo do
  @moduledoc """
  ExAWS implementation of the DynamoClient behavior.
  """
  @behaviour KinesisClient.Leadership.Adapter

  alias ExAws.Dynamo
  require Logger

  @impl true
  def put_leader_item(table_name, worker_id, current_time, opts) do
    # The item we'll insert if we successfully acquire leadership
    item = %{
      "leader_key" => "primary",
      "worker_id" => worker_id,
      "last_update" => current_time
    }

    # Leader lease duration in ms
    lease_duration_ms = 30_000

    # For put_item with a conditional check
    put_opts = [
      condition_expression: "attribute_not_exists(leader_key) OR last_update < :expired_time",
      expression_attribute_values: %{
        expired_time: current_time - lease_duration_ms
      }
    ]

    Dynamo.put_item(table_name, item, put_opts)
    |> ExAws.request(opts)
  end

  @impl true
  def update_leader_heartbeat(table_name, worker_id, current_time, opts) do
    # Key of the item to update
    key = %{"leader_key" => "primary"}

    # Update expression and conditions
    update_opts = [
      condition_expression: "worker_id = :worker_id",
      expression_attribute_values: %{
        worker_id: worker_id,
        current_time: current_time
      },
      update_expression: "SET last_update = :current_time"
    ]

    Dynamo.update_item(table_name, key, update_opts)
    |> ExAws.request(opts)
  end

  @impl true
  def get_leader_info(table_name, opts) do
    Dynamo.get_item(table_name, %{"leader_key" => "primary"})
    |> ExAws.request(opts)
  end

  @impl true
  def describe_table(table_name, opts) do
    Dynamo.describe_table(table_name)
    |> ExAws.request(opts)
  end

  @impl true
  def create_table(table_name, hash_key, key_schema, read_capacity, write_capacity, opts) do
    Dynamo.create_table(
      table_name,
      hash_key,
      key_schema,
      read_capacity,
      write_capacity
    )
    |> ExAws.request(opts)
  end

  @impl true
  def delete_leader_item(table_name, worker_id, opts) do
    Dynamo.delete_item(
      table_name,
      %{"leader_key" => "primary"},
      condition_expression: "worker_id = :worker_id",
      expression_attribute_values: %{worker_id: worker_id}
    )
    |> ExAws.request(opts)
  end
end
