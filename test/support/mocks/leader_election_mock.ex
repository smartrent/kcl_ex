defmodule KinesisClient.LeaderElection.Mock do
  @moduledoc """
  Mock implementation of the LeaderElection module for testing.
  Allows controlling the leadership status for tests.
  """

  use GenServer

  # Client API

  @doc """
  Start the mock LeaderElection service.
  """
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Set the leadership status for a specific app_name.
  """
  def set_leader_status(app_name, is_leader) do
    GenServer.call(__MODULE__, {:set_leader_status, app_name, is_leader})
  end

  @doc """
  Check if the current instance is the leader for the given app_name.
  This is the function that will be mocked in place of the real implementation.
  """
  def is_leader?(app_name) do
    GenServer.call(__MODULE__, {:is_leader?, app_name})
  end

  @doc """
  Reset all leadership statuses.
  """
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  # Server callbacks

  @impl GenServer
  def init(_) do
    {:ok, %{leaders: %{}}}
  end

  @impl GenServer
  def handle_call({:set_leader_status, app_name, is_leader}, _from, state) do
    updated_leaders = Map.put(state.leaders, app_name, is_leader)
    {:reply, :ok, %{state | leaders: updated_leaders}}
  end

  @impl GenServer
  def handle_call({:is_leader?, app_name}, _from, state) do
    # Default to false if app_name is not set
    is_leader = Map.get(state.leaders, app_name, false)
    {:reply, is_leader, state}
  end

  @impl GenServer
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{leaders: %{}}}
  end
end
