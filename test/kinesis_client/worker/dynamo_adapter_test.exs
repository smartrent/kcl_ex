defmodule KinesisClient.Worker.DynamoTest do
  use ExUnit.Case, async: false
  alias KinesisClient.Worker.Dynamo

  # This test requires ExAws to be properly configured with
  # AWS credentials that have DynamoDB permissions.
  #
  # For local testing, we recommend setting up a local DynamoDB instance:
  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

  @moduletag :integration

  # Generate a unique app name for test isolation
  @app_name "test_app_#{System.unique_integer([:positive])}"
  @worker_id "test_worker_#{System.unique_integer([:positive])}"

  # Configuration for local testing
  @dynamo_opts [
    region: "us-east-1",
    access_key_id: "dummy",
    secret_access_key: "dummy",
    endpoint: "http://localhost:8000"
  ]

  setup_all do
    # Initialize the registry for all tests
    Dynamo.initialize(@app_name, @dynamo_opts)

    on_exit(fn ->
      # Clean up the table after tests
      try do
        table_name = "#{@app_name}_worker_registry"
        ExAws.Dynamo.delete_table(table_name) |> ExAws.request(@dynamo_opts)
      rescue
        _ -> :ok
      end
    end)

    :ok
  end

  setup do
    # Clean any existing data between tests
    table_name = "#{@app_name}_worker_registry"

    # Get all items first
    {:ok, %{"Items" => items}} = ExAws.Dynamo.scan(table_name) |> ExAws.request(@dynamo_opts)

    # Delete each item
    Enum.each(items, fn item ->
      ExAws.Dynamo.delete_item(table_name, %{"worker_id" => item["worker_id"]})
      |> ExAws.request(@dynamo_opts)
    end)

    :ok
  end

  test "register_worker stores worker data" do
    metadata = %{
      "hostname" => "test-host",
      "region" => "us-east-1"
    }

    assert :ok = Dynamo.register_worker(@app_name, @worker_id, metadata, @dynamo_opts)

    # Verify worker was stored
    assert {:ok, worker} = Dynamo.get_worker(@app_name, @worker_id, @dynamo_opts)
    assert worker["worker_id"]["S"] == @worker_id
    assert worker["hostname"]["S"] == "test-host"
    assert worker["region"]["S"] == "us-east-1"
    assert worker["is_active"]["BOOL"] == true
    assert is_number(worker["last_heartbeat"]["N"] |> String.to_integer())
  end

  test "heartbeat updates the worker's last_heartbeat time" do
    # First register the worker
    :ok = Dynamo.register_worker(@app_name, @worker_id, %{}, @dynamo_opts)

    # Get initial heartbeat time
    {:ok, worker} = Dynamo.get_worker(@app_name, @worker_id, @dynamo_opts)
    initial_heartbeat = worker["last_heartbeat"]

    # Wait a bit to ensure time changes
    Process.sleep(10)

    # Send heartbeat
    assert :ok = Dynamo.heartbeat(@app_name, @worker_id, @dynamo_opts)

    # Verify heartbeat was updated
    {:ok, updated_worker} = Dynamo.get_worker(@app_name, @worker_id, @dynamo_opts)
    assert updated_worker["last_heartbeat"] > initial_heartbeat
  end

  test "list_active_workers returns only active workers" do
    # Register multiple workers
    active_worker_id = "#{@worker_id}_active"
    inactive_worker_id = "#{@worker_id}_inactive"

    :ok = Dynamo.register_worker(@app_name, active_worker_id, %{}, @dynamo_opts)
    :ok = Dynamo.register_worker(@app_name, inactive_worker_id, %{}, @dynamo_opts)

    # Make one worker inactive
    table_name = "#{@app_name}_worker_registry"

    ExAws.Dynamo.update_item(
      table_name,
      %{"worker_id" => inactive_worker_id},
      update_expression: "SET is_active = :active",
      expression_attribute_values: %{active: false}
    )
    |> ExAws.request(@dynamo_opts)

    # List active workers
    active_workers = Dynamo.list_active_workers(@app_name, @dynamo_opts)

    # Verify only the active worker is returned
    assert Enum.member?(active_workers, active_worker_id)
    refute Enum.member?(active_workers, inactive_worker_id)
  end

  test "remove_worker deletes the worker from the registry" do
    # Register a worker
    :ok = Dynamo.register_worker(@app_name, @worker_id, %{}, @dynamo_opts)

    # Verify it exists
    assert {:ok, _} = Dynamo.get_worker(@app_name, @worker_id, @dynamo_opts)

    # Remove it
    assert :ok = Dynamo.remove_worker(@app_name, @worker_id, @dynamo_opts)

    # Verify it's gone
    assert :not_found = Dynamo.get_worker(@app_name, @worker_id, @dynamo_opts)
  end

  @tag :external
  test "list_all_workers returns all workers regardless of activity status" do
    # Register multiple workers with different states
    active_worker_id = "#{@worker_id}_active"
    inactive_worker_id = "#{@worker_id}_inactive"

    :ok = Dynamo.register_worker(@app_name, active_worker_id, %{status: "active"}, @dynamo_opts)
    :ok = Dynamo.register_worker(@app_name, inactive_worker_id, %{status: "inactive"}, @dynamo_opts)

    # Make one worker inactive
    table_name = "#{@app_name}_worker_registry"

    ExAws.Dynamo.update_item(
      table_name,
      %{"worker_id" => inactive_worker_id},
      update_expression: "SET is_active = :active",
      expression_attribute_values: %{active: false}
    )
    |> ExAws.request(@dynamo_opts)

    # List all workers
    all_workers = Dynamo.list_all_workers(@app_name, @dynamo_opts)

    # Extract worker IDs
    worker_ids =
      Enum.map(all_workers, fn worker ->
        case worker["worker_id"] do
          %{"S" => id} -> id
          id when is_binary(id) -> id
        end
      end)

    # Verify both workers are returned
    assert Enum.member?(worker_ids, active_worker_id)
    assert Enum.member?(worker_ids, inactive_worker_id)
  end
end
