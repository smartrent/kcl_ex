defmodule KinesisClient.SystemHealth do
  @moduledoc """
  System health monitoring for KCL workers.

  This module periodically collects and emits system metrics including:
  - Memory usage
  - Process count
  - Message queue lengths
  - System load
  - Garbage collection statistics
  """

  use GenServer
  require Logger

  # Client API

  @doc """
  Starts the SystemHealth monitor.

  ## Options
    * `:app_name` - Required. The name of the application/stream.
    * `:worker_id` - Required. The ID of the current worker.
    * `:health_check_interval` - Interval in milliseconds between health checks. Default: 30,000ms.
  """
  def start_link(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    GenServer.start_link(__MODULE__, opts, name: name(app_name))
  end

  # Server callbacks

  @impl GenServer
  def init(opts) do
    app_name = Keyword.fetch!(opts, :app_name)
    worker_id = Keyword.fetch!(opts, :worker_id)
    config = Keyword.fetch!(opts, :config)

    health_check_interval =
      Keyword.get(opts, :health_check_interval, config[:system_health_check_interval_ms])

    Logger.info("Starting SystemHealth monitor for #{app_name}, worker: #{worker_id}")

    schedule_health_check(health_check_interval)

    {:ok,
     %{
       app_name: app_name,
       worker_id: worker_id,
       health_check_interval: health_check_interval,
       last_gc_stats: get_gc_stats()
     }}
  end

  @impl GenServer
  def handle_info(:collect_health_metrics, state) do
    collect_and_emit_metrics(state)

    schedule_health_check(state.health_check_interval)

    new_gc_stats = get_gc_stats()
    {:noreply, %{state | last_gc_stats: new_gc_stats}}
  end

  defp schedule_health_check(interval) do
    Process.send_after(self(), :collect_health_metrics, interval)
  end

  defp name(app_name) do
    :"#{__MODULE__}.#{app_name}"
  end

  defp collect_and_emit_metrics(state) do
    emit_memory_metrics(state)
    emit_process_metrics(state)
    emit_message_queue_metrics(state)
    emit_gc_metrics(state)
    emit_system_load_metrics(state)
    emit_worker_metrics(state)
  end

  defp emit_memory_metrics(state) do
    memory_info = :erlang.memory()

    Enum.each(memory_info, fn {type, bytes} ->
      :telemetry.execute(
        [:kinesis_client, :system, :memory],
        %{usage_bytes: bytes},
        %{
          app_name: state.app_name,
          worker_id: state.worker_id,
          type: Atom.to_string(type)
        }
      )
    end)
  end

  defp emit_process_metrics(state) do
    process_count = :erlang.system_info(:process_count)
    process_limit = :erlang.system_info(:process_limit)

    :telemetry.execute(
      [:kinesis_client, :system, :process],
      %{count: process_count, limit: process_limit},
      %{app_name: state.app_name, worker_id: state.worker_id}
    )
  end

  defp emit_message_queue_metrics(state) do
    processes = :erlang.processes()

    queue_lengths =
      processes
      |> Enum.map(fn pid ->
        case :erlang.process_info(pid, :message_queue_len) do
          {:message_queue_len, len} -> len
          nil -> 0
        end
      end)

    max_queue_length = Enum.max(queue_lengths, fn -> 0 end)

    avg_queue_length =
      if length(queue_lengths) > 0, do: Enum.sum(queue_lengths) / length(queue_lengths), else: 0

    :telemetry.execute(
      [:kinesis_client, :system, :message_queue],
      %{max_length: max_queue_length, avg_length: trunc(avg_queue_length)},
      %{app_name: state.app_name, worker_id: state.worker_id}
    )
  end

  defp emit_gc_metrics(state) do
    current_gc_stats = get_gc_stats()

    if state.last_gc_stats do
      gc_count_diff = current_gc_stats.number_of_gcs - state.last_gc_stats.number_of_gcs
      words_reclaimed_diff = current_gc_stats.words_reclaimed - state.last_gc_stats.words_reclaimed

      :telemetry.execute(
        [:kinesis_client, :system, :gc],
        %{
          count: gc_count_diff,
          words_reclaimed: words_reclaimed_diff,
          total_gcs: current_gc_stats.number_of_gcs
        },
        %{app_name: state.app_name, worker_id: state.worker_id}
      )
    end
  end

  defp emit_system_load_metrics(state) do
    try do
      load_info = :cpu_sup.avg1()

      :telemetry.execute(
        [:kinesis_client, :system, :load],
        %{avg1: load_info},
        %{app_name: state.app_name, worker_id: state.worker_id}
      )
    rescue
      _ ->
        :ok
    end

    try do
      case :scheduler.sample_all() do
        {:scheduler_wall_time_all, scheduler_data} when is_list(scheduler_data) ->
          scheduler_count = length(scheduler_data)

          total_utilization =
            Enum.reduce(scheduler_data, 0, fn
              {_type, _id, active_time, total_time}, acc when total_time > 0 ->
                utilization = active_time / total_time * 100
                acc + utilization

              _, acc ->
                acc
            end)

          avg_utilization = if scheduler_count > 0, do: total_utilization / scheduler_count, else: 0

          :telemetry.execute(
            [:kinesis_client, :system, :scheduler],
            %{avg_utilization: avg_utilization, scheduler_count: scheduler_count},
            %{app_name: state.app_name, worker_id: state.worker_id}
          )

        _ ->
          scheduler_count = :erlang.system_info(:schedulers_online)

          :telemetry.execute(
            [:kinesis_client, :system, :scheduler],
            %{scheduler_count: scheduler_count, avg_utilization: 0},
            %{app_name: state.app_name, worker_id: state.worker_id}
          )
      end
    rescue
      _ ->
        :ok
    end
  end

  defp emit_worker_metrics(state) do
    active_workers =
      try do
        KinesisClient.WorkerRegistry.list_active_workers(state.app_name)
      rescue
        _ -> []
      end

    is_registered =
      Enum.any?(active_workers, fn worker ->
        worker_id =
          case worker do
            %{"worker_id" => %{"S" => id}} -> id
            %{"worker_id" => id} -> id
            _ -> nil
          end

        worker_id == state.worker_id
      end)

    :telemetry.execute(
      [:kinesis_client, :worker, :health],
      %{is_registered: if(is_registered, do: 1, else: 0), active_count: length(active_workers)},
      %{app_name: state.app_name, worker_id: state.worker_id}
    )
  end

  defp get_gc_stats do
    case :erlang.statistics(:garbage_collection) do
      {number_of_gcs, words_reclaimed, 0} ->
        %{number_of_gcs: number_of_gcs, words_reclaimed: words_reclaimed}

      {number_of_gcs, words_reclaimed} ->
        %{number_of_gcs: number_of_gcs, words_reclaimed: words_reclaimed}

      _ ->
        %{number_of_gcs: 0, words_reclaimed: 0}
    end
  end
end
