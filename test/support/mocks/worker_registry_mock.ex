defmodule KinesisClient.Worker.MockAdapter do
  @moduledoc """
  Mock implementation of the Worker.Adapter behaviour for testing.
  """
  @behaviour KinesisClient.Worker.Adapter

  use GenServer

  # Client API

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  def update_state(new_state) do
    GenServer.call(__MODULE__, {:update_state, new_state})
  end

  def set_remove_worker_callback(callback) do
    GenServer.call(__MODULE__, {:set_remove_worker_callback, callback})
  end

  def reset_remove_worker_callback do
    GenServer.call(__MODULE__, :reset_remove_worker_callback)
  end

  # Adapter callback implementations

  @impl KinesisClient.Worker.Adapter
  def initialize(app_name, _opts) do
    GenServer.call(__MODULE__, {:initialize, app_name})
  end

  @impl KinesisClient.Worker.Adapter
  def register_worker(app_name, worker_id, metadata, _opts) do
    GenServer.call(__MODULE__, {:register_worker, app_name, worker_id, metadata})
  end

  @impl KinesisClient.Worker.Adapter
  def heartbeat(app_name, worker_id, _opts) do
    GenServer.call(__MODULE__, {:heartbeat, app_name, worker_id})
  end

  @impl KinesisClient.Worker.Adapter
  def list_active_workers(app_name, _opts) do
    GenServer.call(__MODULE__, {:list_active_workers, app_name})
  end

  @impl KinesisClient.Worker.Adapter
  def list_all_workers(app_name, _opts) do
    GenServer.call(__MODULE__, {:list_all_workers, app_name})
  end

  @impl KinesisClient.Worker.Adapter
  def remove_worker(app_name, worker_id, opts) do
    GenServer.call(__MODULE__, {:remove_worker, app_name, worker_id, opts})
  end

  @impl KinesisClient.Worker.Adapter
  def get_worker(app_name, worker_id, _opts) do
    GenServer.call(__MODULE__, {:get_worker, app_name, worker_id})
  end

  # Server callbacks

  @impl GenServer
  def init(_) do
    {:ok, %{workers: %{}, apps: [], remove_worker_callback: nil}}
  end

  @impl GenServer
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl GenServer
  def handle_call(:reset, _from, state) do
    {:reply, :ok, %{workers: %{}, apps: [], remove_worker_callback: state.remove_worker_callback}}
  end

  @impl GenServer
  def handle_call({:update_state, new_state}, _from, state) do
    # Preserve the callback when updating state
    updated_state = Map.put(new_state, :remove_worker_callback, state.remove_worker_callback)
    {:reply, :ok, updated_state}
  end

  @impl GenServer
  def handle_call({:set_remove_worker_callback, callback}, _from, state) do
    {:reply, :ok, %{state | remove_worker_callback: callback}}
  end

  @impl GenServer
  def handle_call(:reset_remove_worker_callback, _from, state) do
    {:reply, :ok, %{state | remove_worker_callback: nil}}
  end

  @impl GenServer
  def handle_call({:initialize, app_name}, _from, state) do
    updated_state = %{state | apps: [app_name | state.apps] |> Enum.uniq()}
    {:reply, :ok, updated_state}
  end

  @impl GenServer
  def handle_call({:register_worker, app_name, worker_id, metadata}, _from, state) do
    key = "#{app_name}:#{worker_id}"
    now = System.system_time(:millisecond)

    # Respect the is_active flag from metadata, default to true if not provided
    is_active = Map.get(metadata, "is_active", true)

    # Respect the last_heartbeat from metadata, default to current time if not provided
    last_heartbeat = Map.get(metadata, "last_heartbeat", now)

    worker =
      Map.merge(metadata, %{
        "worker_id" => worker_id,
        "last_heartbeat" => last_heartbeat,
        "is_active" => is_active
      })

    updated_workers = Map.put(state.workers, key, worker)
    updated_state = %{state | workers: updated_workers}

    {:reply, :ok, updated_state}
  end

  @impl GenServer
  def handle_call({:heartbeat, app_name, worker_id}, _from, state) do
    key = "#{app_name}:#{worker_id}"
    now = System.system_time(:millisecond)

    case Map.get(state.workers, key) do
      nil ->
        # Auto-register the worker if it doesn't exist (like DynamoDB upsert)
        worker = %{
          "worker_id" => worker_id,
          "last_heartbeat" => now,
          "is_active" => true
        }

        updated_workers = Map.put(state.workers, key, worker)
        updated_state = %{state | workers: updated_workers}

        {:reply, :ok, updated_state}

      worker ->
        updated_worker = Map.merge(worker, %{"last_heartbeat" => now, "is_active" => true})
        updated_workers = Map.put(state.workers, key, updated_worker)
        updated_state = %{state | workers: updated_workers}

        {:reply, :ok, updated_state}
    end
  end

  @impl GenServer
  def handle_call({:list_active_workers, app_name}, _from, state) do
    # Consider workers active if they have had a heartbeat in the last minute
    now = System.system_time(:millisecond)
    cutoff = now - 60_000

    active_workers =
      state.workers
      |> Enum.filter(fn {key, worker} ->
        String.starts_with?(key, "#{app_name}:") &&
          worker["is_active"] == true &&
          worker["last_heartbeat"] >= cutoff
      end)
      |> Enum.map(fn {_key, worker} -> worker["worker_id"] end)

    {:reply, active_workers, state}
  end

  @impl GenServer
  def handle_call({:list_all_workers, app_name}, _from, state) do
    all_workers =
      state.workers
      |> Enum.filter(fn {key, _worker} -> String.starts_with?(key, "#{app_name}:") end)
      |> Enum.map(fn {_key, worker} -> worker end)

    {:reply, all_workers, state}
  end

  @impl GenServer
  def handle_call({:remove_worker, app_name, worker_id, opts}, _from, state) do
    # Check if we have a custom callback for remove_worker
    if state.remove_worker_callback do
      result = state.remove_worker_callback.(app_name, worker_id, opts)

      # If the callback returns :ok, also update our internal state
      if result == :ok do
        key = "#{app_name}:#{worker_id}"
        updated_workers = Map.delete(state.workers, key)
        updated_state = %{state | workers: updated_workers}
        {:reply, result, updated_state}
      else
        # Return the result without modifying state
        {:reply, result, state}
      end
    else
      # Default implementation when no callback is set
      key = "#{app_name}:#{worker_id}"

      if Map.has_key?(state.workers, key) do
        updated_workers = Map.delete(state.workers, key)
        updated_state = %{state | workers: updated_workers}

        {:reply, :ok, updated_state}
      else
        {:reply, {:error, :not_found}, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:get_worker, app_name, worker_id}, _from, state) do
    key = "#{app_name}:#{worker_id}"

    case Map.get(state.workers, key) do
      nil -> {:reply, :not_found, state}
      worker -> {:reply, {:ok, worker}, state}
    end
  end
end
