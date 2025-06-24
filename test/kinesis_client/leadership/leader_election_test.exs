defmodule KinesisClient.LeaderElectionTest do
  use ExUnit.Case
  import Mox

  alias KinesisClient.LeaderElection
  alias KinesisClient.Leadership.AdapterMock

  setup :set_mox_global
  setup :verify_on_exit!

  @app_name "test_app"
  @worker_id "worker_123"
  @table_name "#{@app_name}_leader_lock"

  describe "leader election core behavior" do
    setup do
      # Set up a mock adapter that will be used instead of the real DynamoDB adapter
      adapter_opts = [adapter: AdapterMock]

      # Configure the mock to simulate table already exists
      stub(AdapterMock, :describe_table, fn _table_name, _opts ->
        {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}}
      end)

      {:ok, adapter_opts: adapter_opts}
    end

    test "worker becomes leader when no leader exists", %{adapter_opts: adapter_opts} do
      # No leader exists initially
      expect(AdapterMock, :get_leader_info, fn @table_name, _opts ->
        {:ok, %{}}
      end)

      # Expect attempt to take leadership
      expect(AdapterMock, :put_leader_item, fn @table_name, worker_id, current_time, _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      # Start the leader election process
      {:ok, _pid} =
        LeaderElection.start_link(
          app_name: @app_name,
          worker_id: @worker_id,
          dynamo_opts: adapter_opts
        )

      # Let the process handle the first leadership check
      Process.sleep(1500)

      # Verify we're the leader
      assert LeaderElection.is_leader?(@app_name)
    end

    test "worker maintains leadership with heartbeats", %{adapter_opts: adapter_opts} do
      # Mock worker already being the leader
      expect(AdapterMock, :get_leader_info, fn @table_name, _opts ->
        {:ok,
         %{
           # "Item" => %{"worker_id" => @worker_id, "last_update" => System.system_time(:millisecond)}
         }}
      end)

      # Expect heartbeat updates
      expect(AdapterMock, :update_leader_heartbeat, 1, fn @table_name,
                                                          worker_id,
                                                          current_time,
                                                          _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      expect(AdapterMock, :put_leader_item, fn @table_name, worker_id, current_time, _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      # Start the leader election process
      {:ok, _pid} =
        LeaderElection.start_link(
          app_name: @app_name,
          worker_id: @worker_id,
          dynamo_opts: adapter_opts
        )

      # Let it run long enough to send a couple heartbeats
      Process.sleep(7000)

      # Verify we're still the leader
      assert LeaderElection.is_leader?(@app_name)
    end

    test "worker can take over when previous leader has expired", %{adapter_opts: adapter_opts} do
      # Current leader is another worker but has expired
      # 60 seconds old (expired)
      expired_time = System.system_time(:millisecond) - 60_000

      expect(AdapterMock, :get_leader_info, fn @table_name, _opts ->
        {:ok, %{"Item" => %{"worker_id" => "another_worker", "last_update" => expired_time}}}
      end)

      # Expect attempt to take over leadership
      expect(AdapterMock, :put_leader_item, fn @table_name, worker_id, current_time, _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      # Start the leader election process
      {:ok, _pid} =
        LeaderElection.start_link(
          app_name: @app_name,
          worker_id: @worker_id,
          dynamo_opts: adapter_opts
        )

      # Let the process handle the leadership check
      Process.sleep(1500)

      # Verify we've taken over as leader
      assert LeaderElection.is_leader?(@app_name)
    end

    test "worker does not take over when current leader is active", %{adapter_opts: adapter_opts} do
      # Current leader is another worker and is still active
      current_time = System.system_time(:millisecond)

      expect(AdapterMock, :get_leader_info, fn @table_name, _opts ->
        {:ok, %{"Item" => %{"worker_id" => "another_worker", "last_update" => current_time}}}
      end)

      # Start the leader election process
      {:ok, _pid} =
        LeaderElection.start_link(
          app_name: @app_name,
          worker_id: @worker_id,
          dynamo_opts: adapter_opts
        )

      # Let the process handle the leadership check
      Process.sleep(1500)

      # Verify we're not the leader
      refute LeaderElection.is_leader?(@app_name)
    end

    test "worker abandons leadership when requested", %{adapter_opts: adapter_opts} do
      # Worker is currently the leader
      expect(AdapterMock, :get_leader_info, fn @table_name, _opts ->
        {:ok, %{}}
      end)

      expect(AdapterMock, :put_leader_item, fn @table_name, worker_id, current_time, _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      expect(AdapterMock, :update_leader_heartbeat, 2, fn @table_name,
                                                          worker_id,
                                                          current_time,
                                                          _opts ->
        assert worker_id == @worker_id
        assert is_integer(current_time)
        {:ok, %{}}
      end)

      # Expect leadership abandonment
      expect(AdapterMock, :delete_leader_item, fn @table_name, worker_id, _opts ->
        assert worker_id == @worker_id
        {:ok, %{}}
      end)

      # Start the leader election process
      {:ok, _pid} =
        LeaderElection.start_link(
          app_name: @app_name,
          worker_id: @worker_id,
          dynamo_opts: adapter_opts
        )

      # Let the process initialize and become leader
      Process.sleep(7500)

      # Verify we're the leader
      assert LeaderElection.is_leader?(@app_name)

      # Abandon leadership
      LeaderElection.abandon_leadership(@app_name)
      Process.sleep(100)

      # Verify we're no longer the leader
      refute LeaderElection.is_leader?(@app_name)
    end
  end
end
