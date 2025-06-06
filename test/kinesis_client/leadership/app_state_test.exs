defmodule KinesisClient.Leadership.AppStateTest do
  use ExUnit.Case
  import Mox

  alias KinesisClient.Leadership.{AppState, AdapterMock}

  setup :verify_on_exit!

  describe "AppState functions" do
    test "put_leader_item/4 delegates to adapter" do
      expect(AdapterMock, :put_leader_item, fn table, worker_id, time, opts ->
        assert table == "test_table"
        assert worker_id == "worker1"
        assert is_integer(time)
        assert opts == [adapter: AdapterMock]
        {:ok, %{}}
      end)

      opts = [adapter: AdapterMock]
      current_time = System.system_time(:millisecond)

      assert {:ok, %{}} = AppState.put_leader_item("test_table", "worker1", current_time, opts)
    end

    test "update_leader_heartbeat/4 delegates to adapter" do
      expect(AdapterMock, :update_leader_heartbeat, fn table, worker_id, time, opts ->
        assert table == "test_table"
        assert worker_id == "worker1"
        assert is_integer(time)
        assert opts == [adapter: AdapterMock]
        {:ok, %{}}
      end)

      opts = [adapter: AdapterMock]
      current_time = System.system_time(:millisecond)

      assert {:ok, %{}} =
               AppState.update_leader_heartbeat("test_table", "worker1", current_time, opts)
    end

    test "get_leader_info/2 delegates to adapter" do
      expect(AdapterMock, :get_leader_info, fn table, opts ->
        assert table == "test_table"
        assert opts == [adapter: AdapterMock]
        {:ok, %{"Item" => %{"worker_id" => "worker1"}}}
      end)

      opts = [adapter: AdapterMock]

      assert {:ok, %{"Item" => %{"worker_id" => "worker1"}}} =
               AppState.get_leader_info("test_table", opts)
    end

    test "describe_table/2 delegates to adapter" do
      expect(AdapterMock, :describe_table, fn table, opts ->
        assert table == "test_table"
        assert opts == [adapter: AdapterMock]
        {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}}
      end)

      opts = [adapter: AdapterMock]

      assert {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} =
               AppState.describe_table("test_table", opts)
    end

    test "create_table/6 delegates to adapter" do
      expect(AdapterMock, :create_table, fn table, hash_key, key_schema, read, write, opts ->
        assert table == "test_table"
        assert hash_key == "leader_key"
        assert key_schema == %{leader_key: :string}
        assert read == 5
        assert write == 5
        assert opts == [adapter: AdapterMock]
        {:ok, %{}}
      end)

      opts = [adapter: AdapterMock]

      assert {:ok, %{}} =
               AppState.create_table(
                 "test_table",
                 "leader_key",
                 %{leader_key: :string},
                 5,
                 5,
                 opts
               )
    end

    test "delete_leader_item/3 delegates to adapter" do
      expect(AdapterMock, :delete_leader_item, fn table, worker_id, opts ->
        assert table == "test_table"
        assert worker_id == "worker1"
        assert opts == [adapter: AdapterMock]
        {:ok, %{}}
      end)

      opts = [adapter: AdapterMock]

      assert {:ok, %{}} = AppState.delete_leader_item("test_table", "worker1", opts)
    end
  end
end
