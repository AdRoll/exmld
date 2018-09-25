defmodule ElixirProcessor do
  @moduledoc """
  Record processor example implementation.
  """

  require Logger
  require Record
  require Exmld

  defstruct [:flush_interval, :batch_size, :next_flush_time, :pending_items]

  defmodule Token do
    defstruct [:stage, :worker, :sequence_number]
  end

  defmodule Item do
    defstruct [:value, :token]
  end

  @doc """
  Return a flow spec which can be used to set up a processing pipeline; see exmld.ex.
  """
  def flow_spec(stage_names, flow_options, opts \\ []) do
    %{
      stages: stage_names,
      extract_items_fn: &flow_extract/1,
      partition_key: {:elem, 0},
      state0: fn ->
        %__MODULE__{
          flush_interval: opts[:flush_interval] || 10000,
          batch_size: opts[:batch_size] || 10,
          pending_items: [],
          next_flush_time: DateTime.utc_now()
        }
      end,
      process_fn: &flow_process_event/2,
      flow_opts: flow_options
    }
  end

  # flow_extract/1 is called to extract sub-items from a kinesis or dynamo stream record.
  # this allows handling of both KPL-aggregated records and custom aggregation schemes.
  # the output of this function should be a list of 2-tuples ({key, value}) to be passed
  # to flow_process_event/2 for processing in a reducer.
  #
  # items seen by the extract function generally look like this:
  #
  # %Exmld.KinesisStage.Event{
  #   event: %Exmld.KinesisWorker.Datum{
  #       opaque: {"us-west-2", "erlang-processor-kinesis-test"},
  #       shard_id: "shardId-000000000001",
  #       stream_record: {:stream_record, "12345", 946684800,
  #                        {:sequence_number, 12345, 0, :undefined, :undefined},
  #                        " .. record data .. "}},
  #   stage: #PID<0.136.0>,
  #   worker: #PID<0.862.0>}
  #
  defp flow_extract(%Exmld.KinesisStage.Event{
         event: %Exmld.KinesisWorker.Datum{stream_record: record},
         stage: stage,
         worker: worker
       }) do
    case record do
      # handle a heartbeat.  the second element of the tuple will vary so heartbeats get
      # distributed among reducers, so the elements must be swapped since we're using the
      # first element as a partition key.
      {:heartbeat, x} ->
        [{x, :heartbeat}]

      # in a real application, sub-records could be extracted from Event here.  if using a
      # custom non-KPL aggregation scheme, this should associate each sub-record with a
      # faked sequence number having the same base as the parent record, and appropriate
      # 'user_sub' (sub-record index) and 'user_total' (total number of extracted
      # sub-records) fields.  then when later notifying exmld of record disposition, it
      # can properly track sub-record processing and advance the checkpoint beyond the
      # parent record if all of its sub-records were processed.
      #
      # two records having the same key (first tuple element) here will be handled by the
      # same reducer.  in general, the key should be consistently derived from some
      # attribute of the record/item being processed.
      _ when Record.is_record(record, :stream_record) ->
        sn =
          Exmld.stream_record(record, :sequence_number)
          |> Exmld.sequence_number(user_sub: 0)
          |> Exmld.sequence_number(user_total: 1)

        item = %Item{
          value: record,
          token: %Token{stage: stage, worker: worker, sequence_number: sn}
        }

        [{:erlang.phash2(item), item}]
    end
  end

  # process an item extracted from a record (or a heartbeat).  this occurs in a reducer
  # whose initial state is given by 'state0' in flow_spec/3 above.  it returns an updated
  # state after processing the event (and possibly flushing/updating the state
  # accordingly).  here, we simply add the item to the current batch and possibly flush
  # the batch.
  defp flow_process_event({_key, item}, state) do
    state
    |> flow_add_record(item)
    |> maybe_flush()
  end

  defp flow_add_record(state, :heartbeat) do
    state
  end

  defp flow_add_record(%__MODULE__{pending_items: pending} = state, item) do
    %{state | pending_items: [item | pending]}
  end

  # possibly process the current pending batch of records if of the appropriate size or
  # enough time has elapsed:
  defp maybe_flush(state) do
    if should_flush(state) do
      {:ok, state, tokens} = flush(state)
      :ok = notify_dispositions(tokens, :ok)
      note_flush(state)
    else
      state
    end
  end

  defp should_flush(%__MODULE__{
         pending_items: pending,
         batch_size: batch_size,
         next_flush_time: next_flush
       }) do
    length(pending) >= batch_size or DateTime.compare(DateTime.utc_now(), next_flush) != :lt
  end

  # process a batch of items which have been collected, returning {:ok, state, tokens}.
  #
  # `tokens` is a list of tokens used by notify_dispositions/2 to inform upstream workers
  # of the status of processing.  this is needed because a single reducer will potentially
  # receive records from multiple different kinesis shards.  with this disposition scheme,
  # a kinesis worker can correctly checkpoint based on how far along downstream processing
  # has come (instead of for example automatically checkpointing based on time, which
  # could lose records).
  defp flush(%__MODULE__{pending_items: pending} = state) do
    Logger.info("processing batch", items: inspect(pending))
    :timer.sleep(100 * length(pending))
    tokens = for %Item{token: token} <- pending, do: token
    {:ok, %{state | pending_items: []}, tokens}
  end

  # group item processing disposition by origin stage and worker, informing each stage of
  # the records (sequence numbers) from its workers which have been processed.  this
  # allows upstream kinesis workers to safely checkpoint only fully processed data.
  defp notify_dispositions(tokens, status) do
    prepend = fn x -> &[x | &1] end

    List.foldl(tokens, %{}, fn %Token{stage: stage, worker: worker, sequence_number: sn}, acc ->
      d = %Exmld.KinesisWorker.Disposition{sequence_number: sn, status: status}
      Map.update(acc, stage, %{worker => [d]}, &Map.update(&1, worker, [d], prepend.(d)))
    end)
    |> Enum.reduce(:ok, fn {stage, worker_map}, :ok ->
      Exmld.KinesisStage.disposition(stage, worker_map)
    end)
  end

  defp note_flush(%__MODULE__{flush_interval: flush_interval} = state) do
    add = &+/2

    {:ok, next_flush} =
      DateTime.utc_now()
      |> DateTime.to_unix(:millisecond)
      |> add.(flush_interval)
      |> DateTime.from_unix(:millisecond)

    %{state | next_flush_time: next_flush}
  end
end
