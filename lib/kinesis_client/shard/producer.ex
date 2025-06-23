defmodule KinesisClient.Stream.Shard.Producer do
  @moduledoc """
  Producer GenStage used in `KinesisClient.Stream.ShardConsumer` Broadway pipeline.
  """
  use GenStage
  use Retry.Annotation
  require Logger
  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  @behaviour Broadway.Producer

  defstruct [
    :kinesis_opts,
    :stream_name,
    :shard_id,
    :shard_iterator,
    :shard_iterator_type,
    :starting_sequence_number,
    :poll_interval,
    :poll_timer,
    :status,
    :notify_pid,
    :ack_ref,
    :app_name,
    :app_state_opts,
    :lease_owner,
    :shard_closed_timer,
    shutdown_delay: 300_000,
    demand: 0
  ]

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def start(name) do
    Logger.info("Starting producer: #{inspect(name)}", ansi_color: :yellow_background)

    result = GenServer.call(name, :start, 30_000)
    Logger.info("Producer started successfully: #{inspect(name)}, result: #{inspect(result)}")
  end

  def stop(name) do
    Logger.info("Stopping producer: #{inspect(name)}")

    case GenServer.whereis(name) do
      nil ->
        Logger.info("Producer #{inspect(name)} is not running")
        {:ok, :not_running}

      pid ->
        result = GenServer.call(pid, :stop)
        Logger.info("Producer stopped successfully: #{inspect(name)}")
        result
    end
  end

  @impl GenStage
  def init(opts) do
    state = %__MODULE__{
      shard_id: opts[:shard_id],
      app_name: opts[:app_name],
      lease_owner: opts[:lease_owner],
      kinesis_opts: opts[:kinesis_opts],
      stream_name: opts[:stream_name],
      status: opts[:status],
      app_state_opts: Keyword.get(opts, :app_state_opts, []),
      shard_iterator_type: Keyword.get(opts, :shard_iterator_type, :latest),
      poll_interval: Keyword.get(opts, :poll_interval, 5_000),
      notify_pid: Keyword.get(opts, :notify_pid)
    }

    Logger.debug("Starting KinesisClient.Stream.Shard.Producer: #{inspect(state)}")
    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand, status: :stopped} = state) do
    notify({:queuing_demand_while_stopped, incoming_demand}, state)

    {:noreply, [], %{state | demand: demand + incoming_demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand, status: :closed} = state) do
    Logger.info("Shard is closed, not storing demand")
    {:noreply, [], %{state | demand: demand + incoming_demand}}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    Logger.debug("Received incoming demand: #{incoming_demand}")
    get_records(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:get_records, %{poll_timer: nil} = state) do
    Logger.debug("Poll timer is nil")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:get_records, state) do
    notify(:poll_timer_executed, state)

    Logger.debug(
      "Try to fulfill pending
    #{state.demand}: " <>
        "[app_name: #{state.app_name}, shard_id: #{state.shard_id}]"
    )

    get_records(%{state | poll_timer: nil})
  end

  def handle_info(:shard_closed, state) do
    Logger.info(
      "Shard is closed[app_name: #{state.app_name}, " <>
        "shard_id: #{state.shard_id}]"
    )

    KinesisClient.Stream.ShardManager.close_shard(
      state.app_name,
      state.shard_id
    )

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, []}, state) do
    %{metadata: %{"SequenceNumber" => checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    AppState.update_checkpoint(
      state.app_name,
      state.shard_id,
      state.lease_owner,
      checkpoint,
      state.app_state_opts
    )

    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :ack, :success],
      %{
        count: length(successful_msgs)
      },
      %{
        app_name: state.app_name,
        stream_name: state.stream_name,
        shard_id: state.shard_id,
        worker_id: state.lease_owner,
        host: node()
      }
    )

    notify({:acked, %{checkpoint: checkpoint, success: successful_msgs, failed: []}}, state)

    Logger.debug(
      "Acknowledged #{length(successful_msgs)} messages: [app_name: #{state.app_name} " <>
        "shard_id: #{state.shard_id}"
    )

    state = handle_closed_shard(state)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, [], failed_msgs}, state) do
    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :ack, :failure],
      %{
        count: length(failed_msgs)
      },
      %{
        app_name: state.app_name,
        stream_name: state.stream_name,
        shard_id: state.shard_id,
        worker_id: state.lease_owner,
        host: node()
      }
    )

    Logger.debug("Retrying #{length(failed_msgs)} failed messages")

    state =
      case state.shard_closed_timer do
        nil ->
          state

        timer ->
          Process.cancel_timer(timer)
          %{state | shard_closed_timer: nil}
      end

    {:noreply, failed_msgs, state}
  end

  @impl GenStage
  def handle_info({:ack, _ref, successful_msgs, failed_msgs}, state) do
    %{metadata: %{sequence_number: checkpoint}} = successful_msgs |> Enum.reverse() |> hd()

    :ok =
      AppState.update_checkpoint(
        state.shard_id,
        state.lease_owner,
        checkpoint,
        state.app_state_opts
      )

    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :ack, :partial],
      %{
        success_count: length(successful_msgs),
        failure_count: length(failed_msgs)
      },
      %{
        app_name: state.app_name,
        stream_name: state.stream_name,
        shard_id: state.shard_id,
        checkpoint: checkpoint,
        worker_id: state.lease_owner,
        host: node()
      }
    )

    Logger.debug(
      "Acknowledged #{length(successful_msgs)} messages, Retrying #{length(failed_msgs)} failed messages"
    )

    state =
      case state.shard_closed_timer do
        nil ->
          state

        timer ->
          Process.cancel_timer(timer)
          %{state | shard_closed_timer: nil}
      end

    {:noreply, failed_msgs, state}
  end

  @impl GenStage
  def handle_info(msg, state) do
    Logger.debug("ShardConsumer.Producer got an unhandled message #{inspect(msg)}")
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:start, from, %{status: :stopped} = state) do
    Logger.info("Starting producer for shard #{state.shard_id}", ansi_color: :yellow_background)

    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :start],
      %{count: 1, timestamp: System.system_time(:millisecond)},
      %{
        shard_id: state.shard_id,
        worker_id: state.lease_owner
      }
    )

    {:noreply, records, new_state} =
      case AppState.get_lease(state.app_name, state.shard_id, state.app_state_opts) do
        %{checkpoint: nil} ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :latest
          })

        %{checkpoint: "trim_horizon"} ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :trim_horizon
          })

        %{checkpoint: seq_number} when is_binary(seq_number) ->
          get_records(%{
            state
            | status: :started,
              shard_iterator: nil,
              shard_iterator_type: :after_sequence_number,
              starting_sequence_number: seq_number
          })

        :not_found ->
          raise "No lease has been created for #{state.app_name}-#{state.shard_id}"
      end

    GenStage.reply(from, :ok)
    {:noreply, records, new_state}
  end

  @impl GenStage
  def handle_call(:start, from, state) do
    Logger.info("Received start message for shard #{state.shard_id} with status #{state.status}",
      ansi_color: :yellow_background
    )

    GenStage.reply(from, :ok)
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_call(:stop, _from, state) do
    :telemetry.execute(
      [:kinesis_client, :shard, :processing, :end],
      %{timestamp: System.system_time(:millisecond)},
      %{
        app_name: state.app_name,
        stream_name: state.stream_name,
        shard_id: state.shard_id,
        worker_id: state.lease_owner,
        host: node()
      }
    )

    {:reply, :ok, [], %{state | status: :stopped}}
  end

  defp get_records(%__MODULE__{shard_iterator: nil} = state) do
    Logger.debug("Getting initial shard iterator for shard #{state.shard_id}")

    case get_shard_iterator(state) do
      {:ok, %{"ShardIterator" => nil}} ->
        Logger.warning("Received nil shard iterator, marking shard as closed")
        {:noreply, [], %{state | status: :closed}}

      {:ok, %{"ShardIterator" => iterator}} ->
        Logger.debug(
          "Got shard iterator: #{String.slice(iterator, 0, 20)}... for shard #{state.shard_id}"
        )

        get_records(%{state | shard_iterator: iterator})

      error ->
        Logger.error("Failed to get shard iterator for shard #{state.shard_id}: #{inspect(error)}")
        {:noreply, [], state}
    end
  end

  defp get_records(%__MODULE__{demand: 0} = state) do
    Logger.debug("No demand for shard #{state.shard_id}, skipping get_records")
    {:noreply, [], state}
  end

  defp get_records(%__MODULE__{demand: demand, kinesis_opts: kinesis_opts} = state) do
    Logger.debug("Fetching records for shard #{state.shard_id}, demand: #{demand}")

    fetch_result = get_records_with_retry(state, Keyword.merge(kinesis_opts, limit: demand))

    case fetch_result do
      {:ok, %{"Records" => records, "MillisBehindLatest" => millis_behind_latest} = response} ->
        Logger.debug(
          "Fetched #{length(records)} records for shard #{state.shard_id}, millis behind: #{millis_behind_latest}"
        )

        next_iterator = Map.get(response, "NextShardIterator")

        :telemetry.execute(
          [:kinesis_client, :shard, :get_records],
          %{
            millis_behind_latest: millis_behind_latest
          },
          %{
            app_name: state.app_name,
            shard_id: state.shard_id,
            host: node()
          }
        )

        case next_iterator do
          nil ->
            Logger.info("Next iterator is nil, waiting for checklease", ansi_color: :green)
            closed_state = handle_closed_shard(state)

            KinesisClient.Stream.ShardManager.close_shard(
              closed_state.app_name,
              closed_state.shard_id
            )

            {:noreply, [], state}

          _ ->
            messages = wrap_records(records)

            if length(messages) > 0 do
              Logger.info("Produced #{length(messages)} messages for shard #{state.shard_id}",
                ansi_color: :green_background
              )
            end

            new_demand = demand - length(records)

            poll_timer =
              case {records, new_demand} do
                {[], _} ->
                  if state.poll_timer do
                    Process.cancel_timer(state.poll_timer)
                  end

                  Logger.debug("No records received, scheduling poll in #{state.poll_interval}ms")
                  schedule_shard_poll(state.poll_interval)

                {_, 0} ->
                  Logger.debug("Demand satisfied (#{demand} records), not scheduling poll")
                  nil

                _ ->
                  if state.poll_timer do
                    Process.cancel_timer(state.poll_timer)
                  end

                  Logger.debug("Still have demand (#{new_demand}), scheduling immediate poll")
                  schedule_shard_poll(0)
              end

            new_state = %{
              state
              | demand: new_demand,
                poll_timer: poll_timer,
                shard_iterator: next_iterator
            }

            {:noreply, messages, new_state}
        end

      error ->
        Logger.error("Failed to get records for shard #{state.shard_id}: #{inspect(error)}")
        # Schedule a retry after poll interval
        poll_timer = schedule_shard_poll(state.poll_interval)
        {:noreply, [], %{state | poll_timer: poll_timer}}
    end
  end

  @retry with: exponential_backoff(500) |> Stream.take(5)
  defp get_records_with_retry(state, kinesis_opts) do
    Kinesis.get_records(state.shard_iterator, kinesis_opts)
  end

  defp get_shard_iterator(%{shard_iterator_type: :after_sequence_number} = state) do
    Kinesis.get_shard_iterator(
      state.stream_name,
      state.shard_id,
      :after_sequence_number,
      Keyword.put(
        state.kinesis_opts,
        :starting_sequence_number,
        state.starting_sequence_number
      )
    )
  end

  defp get_shard_iterator(%{shard_iterator_type: :trim_horizon} = state) do
    Kinesis.get_shard_iterator(
      state.stream_name,
      state.shard_id,
      :trim_horizon,
      state.kinesis_opts
    )
  end

  defp get_shard_iterator(%{shard_iterator_type: :latest} = state) do
    Kinesis.get_shard_iterator(
      state.stream_name,
      state.shard_id,
      :latest,
      state.kinesis_opts
    )
  end

  # convert Kinesis records to Broadway messages
  defp wrap_records(records) do
    ref = make_ref()

    Enum.map(records, fn %{"Data" => data} = record ->
      metadata = Map.delete(record, "Data")
      acknowledger = {Broadway.CallerAcknowledger, {self(), ref}, nil}
      %Broadway.Message{data: data, metadata: metadata, acknowledger: acknowledger}
    end)
  end

  defp handle_closed_shard(%{status: :closed, shard_closed_timer: nil} = s) do
    timer = Process.send_after(self(), :shard_closed, 5000)

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(
         %{status: :closed, shard_closed_timer: old_timer, shutdown_delay: delay} = s
       ) do
    Process.cancel_timer(old_timer)

    timer = Process.send_after(self(), :shard_closed, delay)

    %{s | shard_closed_timer: timer}
  end

  defp handle_closed_shard(state) do
    state
  end

  defp schedule_shard_poll(interval) do
    Process.send_after(self(), :get_records, interval)
  end

  defp notify(message, %__MODULE__{notify_pid: notify_pid}) do
    case notify_pid do
      pid when is_pid(pid) ->
        send(pid, message)
        :ok

      nil ->
        :ok
    end
  end
end
