defmodule KinesisClient.Leadership.DynamoTest do
  use ExUnit.Case
  use Mimic.DSL

  alias KinesisClient.Leadership.AppState.Dynamo

  # These tests require a local DynamoDB instance
  @moduletag :external

  setup do
    # Use a unique table name for each test run
    table_name = "test_leader_#{:rand.uniform(999_999)}"

    Mimic.stub(KinesisClient.LeaderElection, :is_leader?, fn _app_name ->
      true
    end)

    on_exit(fn ->
      # Clean up the table after tests
      try do
        ExAws.Dynamo.delete_table(table_name) |> ExAws.request()
      catch
        :error, _ -> :ok
      end
    end)

    {:ok, table_name: table_name}
  end

  # @tag :skip
  describe "Dynamo adapter implementation" do
    @tag :external
    test "create_table/6 creates a table", %{table_name: table_name} do
      hash_key = "leader_key"
      key_schema = %{leader_key: :string}
      read_capacity = 5
      write_capacity = 5

      result =
        Dynamo.create_table(
          table_name,
          hash_key,
          key_schema,
          read_capacity,
          write_capacity,
          []
        )

      assert {:ok, %{}} = result

      # Verify the table exists
      {:ok, %{"Table" => table_info}} = ExAws.Dynamo.describe_table(table_name) |> ExAws.request()
      assert table_info["TableName"] == table_name
    end

    @tag :external
    test "put_leader_item/4 puts an item", %{table_name: table_name} do
      # Create the table first
      Dynamo.create_table(
        table_name,
        "leader_key",
        %{leader_key: :string},
        5,
        5,
        []
      )

      # Wait for table to be active
      wait_for_table_active(table_name)

      worker_id = "test_worker_#{:rand.uniform(999)}"
      current_time = System.system_time(:millisecond)

      result =
        Dynamo.put_leader_item(
          table_name,
          worker_id,
          current_time,
          []
        )

      assert {:ok, %{}} = result

      # Verify the item exists
      {:ok, %{"Item" => item}} =
        ExAws.Dynamo.get_item(
          table_name,
          %{"leader_key" => "primary"}
        )
        |> ExAws.request()

      assert item["worker_id"]["S"] == worker_id
    end

    @tag :external
    test "update_leader_heartbeat/4 updates the heartbeat", %{table_name: table_name} do
      # Create the table first
      Dynamo.create_table(
        table_name,
        "leader_key",
        %{leader_key: :string},
        5,
        5,
        []
      )

      # Wait for table to be active
      wait_for_table_active(table_name)

      worker_id = "test_worker_#{:rand.uniform(999)}"
      initial_time = System.system_time(:millisecond)

      # Put initial item
      Dynamo.put_leader_item(
        table_name,
        worker_id,
        initial_time,
        []
      )

      # Wait a bit
      :timer.sleep(100)

      # Update heartbeat
      updated_time = System.system_time(:millisecond)

      result =
        Dynamo.update_leader_heartbeat(
          table_name,
          worker_id,
          updated_time,
          []
        )

      assert {:ok, %{}} = result

      # Verify the item was updated
      {:ok, %{"Item" => item}} =
        ExAws.Dynamo.get_item(
          table_name,
          %{"leader_key" => "primary"}
        )
        |> ExAws.request()

      assert String.to_integer(item["last_update"]["N"]) > initial_time
    end

    @tag :external
    test "get_leader_info/2 retrieves leader info", %{table_name: table_name} do
      # Create the table first
      Dynamo.create_table(
        table_name,
        "leader_key",
        %{leader_key: :string},
        5,
        5,
        []
      )

      # Wait for table to be active
      wait_for_table_active(table_name)

      worker_id = "test_worker_#{:rand.uniform(999)}"
      current_time = System.system_time(:millisecond)

      # Put item
      Dynamo.put_leader_item(
        table_name,
        worker_id,
        current_time,
        []
      )

      # Get leader info
      result = Dynamo.get_leader_info(table_name, [])

      assert {:ok, %{"Item" => item}} = result
      assert item["worker_id"]["S"] == worker_id
    end

    @tag :external
    test "delete_leader_item/3 deletes the leader item", %{table_name: table_name} do
      # Create the table first
      Dynamo.create_table(
        table_name,
        "leader_key",
        %{leader_key: :string},
        5,
        5,
        []
      )

      # Wait for table to be active
      wait_for_table_active(table_name)

      worker_id = "test_worker_#{:rand.uniform(999)}"
      current_time = System.system_time(:millisecond)

      # Put item
      Dynamo.put_leader_item(
        table_name,
        worker_id,
        current_time,
        []
      )

      # Delete item
      result =
        Dynamo.delete_leader_item(
          table_name,
          worker_id,
          []
        )

      assert {:ok, %{}} = result

      # Verify the item is gone
      {:ok, response} =
        ExAws.Dynamo.get_item(
          table_name,
          %{"leader_key" => "primary"}
        )
        |> ExAws.request()

      refute Map.has_key?(response, "Item")
    end
  end

  defp wait_for_table_active(table_name, retries \\ 10, delay \\ 500) do
    if retries <= 0 do
      raise "Timed out waiting for table #{table_name} to become active"
    end

    case ExAws.Dynamo.describe_table(table_name) |> ExAws.request() do
      {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} ->
        :ok

      {:ok, _} ->
        # Table exists but not active yet
        Process.sleep(delay)
        wait_for_table_active(table_name, retries - 1, delay)

      {:error, _} ->
        # Table doesn't exist yet or other error
        Process.sleep(delay)
        wait_for_table_active(table_name, retries - 1, delay)
    end
  end
end
