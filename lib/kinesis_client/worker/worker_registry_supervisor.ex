defmodule KinesisClient.WorkerRegistrySupervisor do
  @moduledoc """
  Supervisor for the worker registry component of KCL.

  This supervisor ensures that the worker registry process stays alive
  and properly restarts if it fails. The worker registry is essential for
  the KCL 3.x architecture's leader-based lease management to discover all
  workers in the system, even those without leases yet.
  """
  use Supervisor
  require Logger

  @doc """
  Starts the worker registry supervisor.

  ## Options
    * `:app_name` - Required. The name of the application/stream.
    * `:worker_id` - Required. The ID of the current worker.
    * All other options are passed to the WorkerRegistry GenServer
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    Supervisor.start_link(__MODULE__, opts, name: name(app_name))
  end

  @impl Supervisor
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)

    Logger.info("Starting worker registry supervisor for #{app_name} with worker #{worker_id}")

    children = [
      # Worker registry process with restart strategy
      {KinesisClient.Worker.Service, opts}
    ]

    # One-for-one ensures that if the worker registry process crashes,
    # it will be restarted without affecting other processes.
    # We use a max_restarts of 5 in 60 seconds to prevent rapid restart loops.
    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 5,
      max_seconds: 60
    )
  end

  defp name(app_name), do: :"#{__MODULE__}.#{app_name}"
end
