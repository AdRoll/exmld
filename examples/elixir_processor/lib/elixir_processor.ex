defmodule ElixirProcessor do
  @moduledoc """
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

  defp flow_extract(%Exmld.KinesisStage.Event{
         event: %Exmld.KinesisWorker.Datum{stream_record: record},
         stage: stage,
         worker: worker
       }) do
    case record do
      {:heartbeat, x} ->
        [{x, :heartbeat}]

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

  defp flush(%__MODULE__{pending_items: pending} = state) do
    Logger.info("processing batch", items: inspect(pending))
    :timer.sleep(100 * length(pending))
    tokens = for %Item{token: token} <- pending, do: token
    {:ok, %{state | pending_items: []}, tokens}
  end

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
