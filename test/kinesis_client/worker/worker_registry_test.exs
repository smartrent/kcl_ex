defmodule KinesisClient.WorkerRegistryTest do
  use ExUnit.Case
  alias KinesisClient.WorkerRegistry
  alias KinesisClient.Worker.MockAdapter

  setup do
    # Start the mock adapter
    {:ok, _pid} = start_supervised(MockAdapter)
    # Reset the mock adapter state between tests
    MockAdapter.reset()

    app_name = "test_app"
    worker_id = "test_worker_1"

    # Set the adapter option to use our mock for all tests
    adapter_opts = [adapter: MockAdapter]

    %{app_name: app_name, worker_id: worker_id, opts: adapter_opts}
  end

  describe "initialize/2" do
    test "successfully initializes the registry", %{app_name: app_name, opts: opts} do
      assert :ok = WorkerRegistry.initialize(app_name, opts)

      # Verify the mock adapter state was updated correctly
      state = MockAdapter.get_state()
      assert app_name in state.apps
    end
  end

  describe "register_worker/4" do
    test "successfully registers a worker with metadata", %{
      app_name: app_name,
      worker_id: worker_id,
      opts: opts
    } do
      # Initialize the registry first
      :ok = WorkerRegistry.initialize(app_name, opts)

      # Create some metadata for the worker
      metadata = %{
        "hostname" => "test-host",
        "pid" => "12345"
      }

      # Register the worker
      assert :ok = WorkerRegistry.register_worker(app_name, worker_id, metadata, opts)

      # Verify the worker was registered correctly
      case WorkerRegistry.get_worker(app_name, worker_id, opts) do
        {:ok, worker} ->
          assert worker["worker_id"] == worker_id
          assert worker["hostname"] == "test-host"
          assert worker["pid"] == "12345"
          assert worker["is_active"] == true
          assert is_integer(worker["last_heartbeat"])

        other ->
          flunk("Expected {:ok, worker} but got #{inspect(other)}")
      end
    end
  end

  describe "heartbeat/3" do
    test "successfully updates worker heartbeat", %{
      app_name: app_name,
      worker_id: worker_id,
      opts: opts
    } do
      # Setup: initialize and register worker
      :ok = WorkerRegistry.initialize(app_name, opts)
      :ok = WorkerRegistry.register_worker(app_name, worker_id, %{}, opts)

      # Get the initial state
      {:ok, initial_worker} = WorkerRegistry.get_worker(app_name, worker_id, opts)
      initial_heartbeat = initial_worker["last_heartbeat"]

      # Wait a bit to ensure heartbeat time will be different
      :timer.sleep(10)

      # Send a heartbeat
      assert :ok = WorkerRegistry.heartbeat(app_name, worker_id, opts)

      # Verify the heartbeat was updated
      {:ok, updated_worker} = WorkerRegistry.get_worker(app_name, worker_id, opts)
      assert updated_worker["last_heartbeat"] > initial_heartbeat
    end
  end

  describe "list_active_workers/2" do
    test "returns only active workers", %{app_name: app_name, opts: opts} do
      :ok = WorkerRegistry.initialize(app_name, opts)

      # Register multiple workers
      :ok = WorkerRegistry.register_worker(app_name, "worker1", %{}, opts)
      :ok = WorkerRegistry.register_worker(app_name, "worker2", %{}, opts)
      :ok = WorkerRegistry.register_worker(app_name, "worker3", %{}, opts)

      # Make worker2 inactive by manipulating the mock adapter state directly
      # (This is a test-only approach to simulate an inactive worker)
      state = MockAdapter.get_state()
      key = "#{app_name}:worker2"
      worker = Map.get(state.workers, key)
      worker = Map.put(worker, "is_active", false)
      updated_workers = Map.put(state.workers, key, worker)
      MockAdapter.update_state(%{state | workers: updated_workers})

      # List active workers
      active_workers = WorkerRegistry.list_active_workers(app_name, opts)

      # Verify only worker1 and worker3 are active
      assert length(active_workers) == 2
      assert "worker1" in active_workers
      assert "worker3" in active_workers
      refute "worker2" in active_workers
    end
  end

  describe "list_all_workers/2" do
    test "returns all workers regardless of status", %{app_name: app_name, opts: opts} do
      :ok = WorkerRegistry.initialize(app_name, opts)

      # Register multiple workers
      :ok = WorkerRegistry.register_worker(app_name, "worker1", %{"group" => "A"}, opts)
      :ok = WorkerRegistry.register_worker(app_name, "worker2", %{"group" => "B"}, opts)

      # List all workers
      all_workers = WorkerRegistry.list_all_workers(app_name, opts)

      # Verify all workers are returned
      assert length(all_workers) == 2

      # Extract worker_ids from the result
      worker_ids = Enum.map(all_workers, fn worker -> worker["worker_id"] end)
      assert "worker1" in worker_ids
      assert "worker2" in worker_ids

      # Verify metadata is preserved
      worker1 = Enum.find(all_workers, fn w -> w["worker_id"] == "worker1" end)
      worker2 = Enum.find(all_workers, fn w -> w["worker_id"] == "worker2" end)
      assert worker1["group"] == "A"
      assert worker2["group"] == "B"
    end
  end

  describe "remove_worker/3" do
    test "successfully removes a worker", %{app_name: app_name, worker_id: worker_id, opts: opts} do
      :ok = WorkerRegistry.initialize(app_name, opts)
      :ok = WorkerRegistry.register_worker(app_name, worker_id, %{}, opts)

      # Verify worker exists
      assert {:ok, _} = WorkerRegistry.get_worker(app_name, worker_id, opts)

      # Remove the worker
      assert :ok = WorkerRegistry.remove_worker(app_name, worker_id, opts)

      # Verify worker no longer exists
      assert :not_found = WorkerRegistry.get_worker(app_name, worker_id, opts)
    end

    test "returns error when removing non-existent worker", %{app_name: app_name, opts: opts} do
      :ok = WorkerRegistry.initialize(app_name, opts)

      # Try to remove a non-existent worker
      assert {:error, :not_found} = WorkerRegistry.remove_worker(app_name, "non_existent", opts)
    end
  end
end
