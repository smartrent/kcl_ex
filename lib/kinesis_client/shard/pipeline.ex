defmodule KinesisClient.Stream.Shard.Pipeline do
  @moduledoc false
  use Broadway
  import KinesisClient.Util
  alias KinesisClient.Stream.Shard.Producer
  require Logger

  def start_link(opts) do
    producer_opts = [
      app_name: opts[:app_name],
      shard_id: opts[:shard_id],
      lease_owner: opts[:lease_owner],
      stream_name: opts[:stream_name],
      kinesis_opts: Keyword.get(opts, :kinesis_opts, []),
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      coordinator_name: opts[:coordinator_name],
      status: :stopped
    ]

    min_demand = Keyword.get(opts, :min_demand, 10)
    max_demand = Keyword.get(opts, :max_demand, 20)
    batch_size = Keyword.get(opts, :batch_size, 20)

    processor_concurrency = Keyword.get(opts, :processor_concurrency, 1)
    batcher_concurrency = Keyword.get(opts, :batcher_concurrency, 1)

    processor_opts =
      Keyword.get(opts, :processors,
        default: [
          concurrency: processor_concurrency,
          min_demand: min_demand,
          max_demand: max_demand
        ]
      )

    batcher_opts =
      Keyword.get(opts, :batchers,
        default: [concurrency: batcher_concurrency, batch_size: batch_size]
      )

    # pipeline context must be a map
    pipeline_context =
      opts
      |> Keyword.get(:pipeline_context, %{})
      |> Map.put(:shard_consumer, opts[:shard_consumer])

    pipeline_opts = [
      name: name(opts[:app_name], opts[:shard_id]),
      producer: [
        module: {Producer, producer_opts},
        concurrency: 1
      ],
      context: pipeline_context,
      processors: processor_opts,
      batchers: batcher_opts
    ]

    pipeline_opts = optional_kw(pipeline_opts, :partition_by, Keyword.get(opts, :partition_by))

    Broadway.start_link(__MODULE__, pipeline_opts)
  end

  def start(app_name, shard_id) do
    Logger.info("Starting pipeline for app: #{app_name}, shard: #{shard_id}",
      ansi_color: :blue_background
    )

    names = Broadway.producer_names(name(app_name, shard_id))

    Logger.debug("Producer names: #{inspect(names)}")

    errors =
      Enum.reduce(names, [], fn name, errs ->
        Logger.debug("Starting producer: #{inspect(name)}")

        case Producer.start(name) do
          :ok ->
            Logger.info("Successfully started producer: #{inspect(name)}")
            errs

          other ->
            Logger.error("Failed to start producer: #{inspect(name)}, error: #{inspect(other)}")
            [other | errs]
        end
      end)

    case errors do
      [] ->
        Logger.info("Successfully started all producers for shard #{shard_id}")
        :ok

      errors ->
        Logger.error("Failed to start some producers for shard #{shard_id}: #{inspect(errors)}")
        errors
    end
  end

  def stop(app_name, shard_id) do
    pipeline_name = name(app_name, shard_id)

    # Check if the pipeline exists before trying to stop it
    if GenServer.whereis(pipeline_name) do
      # First stop the producers to prevent new messages from being fetched
      names = Broadway.producer_names(pipeline_name)

      Enum.each(names, fn name ->
        if GenServer.whereis(name) do
          Producer.stop(name)
        end
      end)

      # Allow enough time for any in-flight messages to complete processing
      # If consumer processing time is 5 seconds, we need at least that much
      # Plus some buffer for Broadway's internal processing
      Logger.info(
        "Waiting for in-flight messages to complete for pipeline: #{inspect(pipeline_name)}"
      )

      # 6 seconds (5s processing + 1s buffer)
      Process.sleep(6000)

      # Then completely stop the Broadway pipeline
      Logger.info("Stopping pipeline: #{inspect(pipeline_name)}")

      try do
        Broadway.stop(pipeline_name)
        Logger.info("Successfully stopped pipeline for #{shard_id}")
        :ok
      catch
        :exit, {:normal, _} ->
          # This is expected and not an error - Broadway is sending a normal exit signal
          Logger.info("Broadway pipeline for #{shard_id} exited normally")
          :ok

        _kind, reason ->
          Logger.error("Error stopping Broadway pipeline for #{shard_id}: #{inspect(reason)}")
          {:error, reason}
      end
    else
      Logger.info("Pipeline #{inspect(pipeline_name)} is not running")
      :ok
    end
  end

  @doc """
  Stop a pipeline given just the pipeline PID.
  This is a convenience function for supervisor-based stopping.
  """
  def stop(pipeline_pid) when is_pid(pipeline_pid) do
    try do
      # Try to stop the pipeline the proper way
      if Process.alive?(pipeline_pid) do
        # Get the pipeline's registered name if possible
        case Process.info(pipeline_pid, :registered_name) do
          {:registered_name, name} when is_atom(name) ->
            # If the name follows our pattern, extract the components
            Process.exit(Atom.to_string(name), :shutdown)
            :ok

          _ ->
            # No registered name, just kill it
            Process.exit(pipeline_pid, :shutdown)
            :ok
        end
      else
        :ok
      end
    rescue
      e ->
        Logger.warning("Error stopping pipeline PID #{inspect(pipeline_pid)}: #{inspect(e)}")
        # Last resort - force kill it
        if Process.alive?(pipeline_pid) do
          Process.exit(pipeline_pid, :kill)
        end

        :ok
    end
  end

  @impl Broadway
  def handle_message(processor, msg, ctx) do
    module = Map.get(ctx, :shard_consumer)
    module.handle_message(processor, msg, ctx)
  end

  @impl Broadway
  def handle_batch(batcher, messages, batch_info, context) do
    module = Map.get(context, :shard_consumer)

    module.handle_batch(batcher, messages, batch_info, context)
  end

  @impl Broadway
  def handle_failed(messages, context) do
    module = Map.get(context, :shard_consumer)

    module.handle_failed(messages, context)
  end

  def name(app_name, shard_id) do
    Module.concat([KinesisClient.Stream.Shard.Pipeline, app_name, shard_id])
  end
end
