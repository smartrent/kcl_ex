defmodule KinesisClient.Stream.AppState.Mock do
  @moduledoc """
  Mock implementation of the AppState module for testing.
  Allows controlling the app state for tests.
  """

  use GenServer

  # Client API

  @doc """
  Start the mock AppState service.
  """
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Reset the mock state.
  """
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @doc """
  Set the leases that will be returned for a specific worker.
  """
  def set_worker_leases(app_name, worker_id, leases) do
    GenServer.call(__MODULE__, {:set_worker_leases, app_name, worker_id, leases})
  end

  @doc """
  Mock implementation of list_worker_leases/2.
  """
  def list_worker_leases(app_name, worker_id) do
    GenServer.call(__MODULE__, {:list_worker_leases, app_name, worker_id})
  end

  # Server callbacks

  @impl GenServer
  def init(_) do
    {:ok, %{worker_leases: %{}, lease_operations: []}}
  end

  @impl GenServer
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{worker_leases: %{}, lease_operations: []}}
  end

  @impl GenServer
  def handle_call({:set_worker_leases, app_name, worker_id, leases}, _from, state) do
    key = "#{app_name}:#{worker_id}"
    updated_leases = Map.put(state.worker_leases, key, leases)
    {:reply, :ok, %{state | worker_leases: updated_leases}}
  end

  @impl GenServer
  def handle_call({:list_worker_leases, app_name, worker_id}, _from, state) do
    key = "#{app_name}:#{worker_id}"
    leases = Map.get(state.worker_leases, key, [])
    {:reply, leases, state}
  end

  @doc """
  Get all recorded lease operations.
  """
  def get_lease_operations do
    GenServer.call(__MODULE__, :get_lease_operations)
  end

  @impl GenServer
  def handle_call(:get_lease_operations, _from, state) do
    {:reply, state.lease_operations, state}
  end
end
