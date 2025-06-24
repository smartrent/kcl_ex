defmodule KinesisClient.Stream.LeaseRefresherTest do
  use ExUnit.Case, async: false
  use Mimic

  alias KinesisClient.Stream.LeaseRefresher
  alias KinesisClient.Stream.AppState

  @app_name "test_app"
  @worker_id "test-worker-1"

  setup :verify_on_exit!

  describe "initialization" do
    test "initializes with default intervals" do
      # Start the refresher with minimal options
      {:ok, state} = LeaseRefresher.init(app_name: @app_name, worker_id: @worker_id)

      # Assert default values are set
      assert state.app_name == @app_name
      assert state.worker_id == @worker_id
      assert state.lease_take_interval == 20_000
      assert state.lease_renew_interval == 10_000
      assert is_reference(state.renew_timer)
      assert state.running == true
    end

    test "uses custom intervals when provided" do
      # Custom intervals
      lease_take_interval = 30_000
      lease_renew_interval = 15_000

      # Start the refresher with custom intervals
      {:ok, state} =
        LeaseRefresher.init(
          app_name: @app_name,
          worker_id: @worker_id,
          lease_take_interval: lease_take_interval,
          lease_renew_interval: lease_renew_interval
        )

      # Assert custom values are used
      assert state.lease_take_interval == lease_take_interval
      assert state.lease_renew_interval == lease_renew_interval
    end
  end

  describe "lease renewal" do
    test "successfully renews worker leases" do
      # Setup mocks for AppState
      leases = [
        %{shard_id: "shard-1", lease_count: 1},
        %{shard_id: "shard-2", lease_count: 2}
      ]

      AppState
      |> expect(:list_worker_leases, fn app_name, worker_id ->
        assert app_name == @app_name
        assert worker_id == @worker_id
        leases
      end)

      # Mock successful lease renewals
      AppState
      |> expect(:renew_lease, 2, fn app_name, lease ->
        assert app_name == @app_name
        assert lease in leases
        {:ok, lease.lease_count + 1}
      end)

      # Initial state
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: nil,
        take_timer: nil,
        running: true
      }

      # Send the renewal message
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify a new timer was scheduled
      assert is_reference(updated_state.renew_timer)
    end

    test "handles lease renewal failures" do
      # Setup mocks for AppState
      leases = [
        %{shard_id: "shard-1", lease_count: 1},
        %{shard_id: "shard-2", lease_count: 2},
        %{shard_id: "shard-3", lease_count: 3}
      ]

      AppState
      |> expect(:list_worker_leases, fn app_name, worker_id ->
        assert app_name == @app_name
        assert worker_id == @worker_id
        leases
      end)

      # Mock successful lease renewals for first two shards
      AppState
      |> expect(:renew_lease, fn app_name, %{shard_id: "shard-1"} = lease ->
        assert app_name == @app_name
        {:ok, lease.lease_count + 1}
      end)

      AppState
      |> expect(:renew_lease, fn app_name, %{shard_id: "shard-2"} = lease ->
        assert app_name == @app_name
        {:ok, lease.lease_count + 1}
      end)

      # Mock failure for the third shard
      AppState
      |> expect(:renew_lease, fn app_name, %{shard_id: "shard-3"} ->
        assert app_name == @app_name
        {:error, :conditional_check_failed}
      end)

      # Initial state
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: nil,
        take_timer: nil,
        running: true
      }

      # Send the renewal message
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify a new timer was scheduled
      assert is_reference(updated_state.renew_timer)
    end

    test "skips lease renewal when not running" do
      # Setup state with running=false
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: nil,
        take_timer: nil,
        running: false
      }

      # Send the renewal message
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify state remains unchanged
      assert updated_state == state
    end

    test "cancels existing timer before starting a new one" do
      # Create a timer that won't fire during the test
      real_timer_ref = Process.send_after(self(), :test_timer, 60_000)

      # Setup mocks for AppState
      AppState
      |> expect(:list_worker_leases, fn _, _ -> [] end)

      # Initial state with an existing timer
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: real_timer_ref,
        take_timer: nil,
        running: true
      }

      # Send the renewal message
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify a new timer was scheduled and is different from the original
      assert is_reference(updated_state.renew_timer)
      assert updated_state.renew_timer != real_timer_ref

      # Clean up the timer
      Process.cancel_timer(real_timer_ref)
      # Flush any messages that might have been sent
      receive do
        :test_timer -> :ok
      after
        0 -> :ok
      end
    end
  end

  describe "start_link/1" do
    test "starts a process with required options" do
      # Create a unique app name for this test to avoid conflicts
      unique_app_name = "#{@app_name}_#{System.unique_integer([:positive])}"

      # Mock AppState to prevent actual lease operations
      AppState
      |> stub(:list_worker_leases, fn _, _ -> [] end)

      # We need to supervise the process so it gets cleaned up after the test
      child_spec = %{
        id: LeaseRefresher,
        start: {LeaseRefresher, :start_link, [[app_name: unique_app_name, worker_id: @worker_id]]},
        restart: :temporary
      }

      # Start under a temporary supervisor
      {:ok, sup_pid} = Supervisor.start_link([child_spec], strategy: :one_for_one)

      # Find the LeaseRefresher process
      [{_, pid, _, _}] = Supervisor.which_children(sup_pid)

      # Verify the process is running
      assert Process.alive?(pid)

      # Clean up by stopping the supervisor
      Supervisor.stop(sup_pid)
    end

    test "validates required options" do
      # Test the init function directly instead of start_link to avoid process registration issues
      assert_raise KeyError, fn ->
        LeaseRefresher.init(worker_id: @worker_id)
      end

      assert_raise KeyError, fn ->
        LeaseRefresher.init(app_name: @app_name)
      end
    end
  end

  describe "handling complex renewal scenarios" do
    test "handles unexpected errors during lease renewal" do
      # Setup mocks for AppState
      leases = [%{shard_id: "shard-1", lease_count: 1}]

      AppState
      |> expect(:list_worker_leases, fn _, _ -> leases end)

      # Mock an unexpected error during renewal
      AppState
      |> expect(:renew_lease, fn _, _ ->
        # Simulate unexpected error
        {:unexpected, :error_format}
      end)

      # Initial state
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: nil,
        take_timer: nil,
        running: true
      }

      # Send the renewal message - should handle error gracefully
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify a new timer was scheduled
      assert is_reference(updated_state.renew_timer)
    end

    test "handles empty lease list" do
      # Setup mocks for AppState - no leases held
      AppState
      |> expect(:list_worker_leases, fn _, _ -> [] end)

      # Initial state
      state = %{
        app_name: @app_name,
        worker_id: @worker_id,
        app_state_opts: [],
        lease_take_interval: 20_000,
        lease_renew_interval: 10_000,
        renew_timer: nil,
        take_timer: nil,
        running: true
      }

      # Send the renewal message - should handle empty list
      {:noreply, updated_state} = LeaseRefresher.handle_info(:renew_leases, state)

      # Verify a new timer was scheduled
      assert is_reference(updated_state.renew_timer)
    end
  end
end
