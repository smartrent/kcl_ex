defmodule KinesisClient.LeaderElection.Supervisor do
  @moduledoc """
  Supervisor for the leader election components in KinesisClient.
  """
  use Supervisor
  require Logger

  @doc """
  Starts the leader election supervisor.

  ## Options
    * `:app_name` - Required. The name of the application
    * `:worker_id` - Required. Unique identifier for this worker
    * All other options are passed to the LeaderElection GenServer
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    Supervisor.start_link(__MODULE__, opts, name: name(app_name))
  end

  @impl Supervisor
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)

    Logger.info("Starting leader election supervisor for #{app_name} with worker #{worker_id}")

    children = [
      {KinesisClient.LeaderElection, opts}
    ]

    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 5,
      max_seconds: 60
    )
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
