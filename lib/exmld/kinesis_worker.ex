defmodule Exmld.KinesisWorker do
  require Logger
  require Exmld
  @behaviour :erlmld_flusher

  defstruct [{:stages, []},
             {:shard_id, nil},
             {:opaque, nil},
             {:counter, 0},
             {:heartbeats, 0},
             {:errors, 0},
             {:error_callback, nil},
             {:skip_errors, false},
             {:done, []},
             {:max_pending, 1000},
             {:await_sleep_interval, 1000},
             {:pending, %{}}]

  @type flusher_token :: any
  @type t :: %__MODULE__{# list of identifiers which can be `GenStage.call/3`ed:
                         stages: [any],
                         shard_id: Exmld.shard_id,
                         opaque: any,
                         counter: non_neg_integer,
                         heartbeats: non_neg_integer,
                         errors: non_neg_integer,
                         error_callback: ((t, [Exmld.KinesisWorker.Disposition.t]) -> any)
                                         | nil,
                         skip_errors: boolean,
                         done: [flusher_token],
                         max_pending: pos_integer,
                         await_sleep_interval: non_neg_integer,
                         # map from a sequence number to a token or a tuple of a token and
                         # a list of {sub, total} values known so far.
                         #
                         # if we expect only a single disposition for a record, it was a
                         # kpl sub-record received from upstream and we store a value
                         # consisting of the associated token here.
                         #
                         # otherwise, we store {token, []} and await all outstanding
                         # dispositions for items extracted from the originating record.
                         pending: %{optional(Exmld.sequence_number) =>
                                     flusher_token | {flusher_token, [{non_neg_integer,
                                                                       non_neg_integer}]}}}

  @moduledoc """
  An [erlmld_flusher](https://github.com/AdRoll/erlmld/blob/master/src/erlmld_flusher.erl)
  which can interface with a `Exmld.KinesisStage` data source.

  This implements an `erlmld_flusher` which can be used by `erlmld_batch_processor`.
  Unlike a typical `erlmld_flusher`, it has a different notion of fullness: if more than
  `:max_pending` items are in flight, the worker waits for all pending items before
  emitting any more for downstream processing.  A periodic flush interval should be
  configured in the batch processor options.  Similarly, the downstream stage processing
  pipeline should not require any kind of "full" condition and should periodically make
  progress (i.e., emit/flush output) even if no more records are sent.

  Heartbeat items are sent while the worker is waiting for pending items to be completed;
  these include varying counters to allow them to be automatically distributed among
  downstream reducers.

  One worker process will exist for each stream shard owned by the current node.  Each
  such process will have been configured with a set of downstream `Exmld.KinesisStage`s
  which can receive records from it (actually `Exmld.KinesisWorker.Datum`s); those stages
  will be part of a data processing `Flow.t`.  Eventually, the disposition of each
  record's processing will propagate back to the originating worker (as return values from
  `GenStage.call/3`).

  Periodically, `erlmld_batch_processor` will request a flush.  If the flush kind is
  `:partial`, we return the tokens associated with the records which have already been
  fully processed.  Otherwise, the flush kind is `:full` and we await the disposition of
  every outstanding record before returning.

  If processing of any record (or item extracted therefrom) fails, the worker will crash
  unless it's configured to ignore processing errors.

  Records presented to this worker may be ordinary records or sub-records extracted from a
  containing KPL-aggregated record.  If KPL aggregation is not being used, but smaller
  sub-items are later extracted by the stage processing pipeline, the pipeline should
  create fake sub-record sequence numbers to track the disposition of those items (and
  sub-record checkpointing should be turned off).

  Periodically (which should be at some multiple of the periodic flush interval),
  `erlmld_batch_processor` will checkpoint based on the records which have so far been
  successfully processed (those whose tokens have been returned from `flush/2`).
  """

  defmodule Datum do
    @doc """
    Struct for annotating stream records (or heartbeats) with additional data.

    To allow a single downstream processing pipeline to be used with multiple source
    streams, we annotate Kinesis stream records with additional data before providing them
    to the stage(s) processing them.  (Example: `:opaque` could be a 2-tuple naming the
    source stream and region or otherwise indicate how to specially process the record).

    ## Fields

     * `:opaque`        - the opaque term provided at worker init time
     * `:shard_id`      - name of the shard the worker is processing
     * `:stream_record` - a record from the stream or `{:heartbeat, _}`
    """
    defstruct opaque: nil, shard_id: nil, stream_record: nil
    @type t :: %Datum{opaque: any,
                      shard_id: Exmld.shard_id,
                      stream_record: Exmld.stream_record | {:heartbeat, any}}
  end

  defmodule Disposition do
    @doc """
    Struct for event processing disposition.

    Tracks whether processing succeeded or failed for a specific record or item extracted
    therefrom.

    ## Fields

      * `:sequence_number` - `Exmld.sequence_number()` of the subject record.  If the
      subject is an item extracted from a containing aggregate record, the `sub` and
      `total` fields should be populated (whether KPL aggregation was used or not).
      * `:status`          - processing status
    """
    defstruct sequence_number: nil, status: nil
    @type t :: %Disposition{sequence_number: Exmld.sequence_number,
                            status: :ok | {:error, term}}
  end

  @doc """
  Initialize worker state with a shard id and a set of options.

  An `erlmld_batch_processor` is initializing processing on `shard_id` and providing the
  `flusher_mod_data` which was passed to it, which should be an enumerable of `keyword`s
  containing the following options; we return a flusher state to be used in subsequent
  operations.

  ## Options

  All optional unless marked required:

    * `:stages` - (required) list of `GenStage`s (values useable as first arg to
      `GenStage.call/3`) which can receive `Exmld.KinesisWorker.Datum`s
    * `:opaque` - opaque term passed in each `Exmld.KinesisWorker.Datum`
    * `:skip_errors` - boolean indicating whether errors are non-fatal (if false, crash on
       error).
    * `:max_pending` - maximum number of pending items which can be in flight.
    * `:await_sleep_interval` - sleep time between checks while awaiting pending items.
    * `:error_callback` - `nil` or an arity-2 function called with state and failure
      dispositions when processing failures occur.
  """
  def init(shard_id, opts) do
    unless length(opts[:stages] || []) > 0 do
      exit(:no_stages_configured)
    end
    Logger.metadata(shard_id: shard_id, opaque: opts[:opaque])
    struct(%__MODULE__{error_callback: &(log_errors(&1, &2)),
                       shard_id: shard_id}, Map.new(opts))
  end

  @doc """
  Submit a new Kinesis record to the downstream pipeline for processing.

  A new Kinesis record is available for processing, and `erlmld_batch_processor` is
  instructing us to add it to the current batch.  Since we really have no notion of a
  batch, we immediately choose a downstream stage and notify it of a new
  `Exmld.KinesisWorker.Datum` containing the record and make a note of it being in-flight.
  That call will block until a further-downstream consumer receives the record as a flow
  event.

  The result of that call will be an updated list of item dispositions.  Unless configured
  to skip records which failed to be processed, we crash if any failed.  Otherwise we
  update the set of done/pending items and return an updated state.
  """
  def add_record(%__MODULE__{max_pending: max_pending,
                             pending: pending} = state, record, token)
  when map_size(pending) >= max_pending do
    Logger.info("#{state.shard_id} has too many pending items, awaiting...")
    add_record(await_pending(state), record, token)
  end
  def add_record(state, record, token) do
    state =
      state
      |> incr(:counter, 1)
      |> note_pending(record, token)
      |> notify_downstream(record)
      |> update_pending()
    {:ok, state}
  end

  @doc """
  Return a list of tokens corresponding to records which have been fully processed and the
  latest state.

  If the flush kind is `:full`, we await the disposition of all outstanding records before
  returning.  Otherwise, it's `:partial` and we return (possibly an empty result)
  immediately.

  If doing a full flush and any records fail to be successfully processed, we crash unless
  configured to skip failed records.
  """
  def flush(state, kind) do
    %__MODULE__{done: done} = state = case kind do
                                        :partial ->
                                          state
                                        :full ->
                                          await_pending(state)
                                      end
    {:ok, %{state | done: []}, done}
  end

  # a record and token have been provided.  if the record is a kpl sub-record, it has
  # base, sub, and total fields populated, and we expect a single disposition for it.
  # otherwise, it's a normal record which will later have items extracted from it (we
  # don't know how many), and we'll expect multiple dispositions for it (each containing a
  # faked sequence number also containing populated base, sub, and total); once we receive
  # all of those, it's done.
  defp note_pending(%__MODULE__{pending: pending} = state, record, token) do
    sn = Exmld.stream_record(record, :sequence_number)
    if pending[sn] do
      # we received the same sequence number for two records; this should not happen.
      exit({:duplicate_seqno, sn})
    end
    expect_multiple = :undefined == Exmld.sequence_number(sn, :user_sub)
    stored_token = maybe_standard_token(token, sn)
    %{state | pending: Map.put(pending, sn, case expect_multiple do
                                              true ->
                                                {stored_token, []}
                                              false ->
                                                stored_token
                                            end)}
  end

  # take advantage of the normal (but opaque and currently unsupported) {N, SN}
  # representation of tokens to avoid storing the sequence number twice:
  defp maybe_standard_token({n, sn}, sn_) when sn == sn_ do
    {:t, n}
  end
  defp maybe_standard_token(t, _) do
    t
  end

  defp maybe_wrap_token({:t, n}, sn) do
    {n, sn}
  end
  defp maybe_wrap_token(t, _) do
    t
  end

  # a list of finished sequence numbers has been provided.  either:
  #
  # 1. we received a kpl sub-record from upstream and it was passed to a reducer.  we are
  #    now receiving a sequence number with base, sub, and total fields populated.  that
  #    sub-record would have been associated with one flusher token, which is now done.
  #
  # or:
  #
  # 2. we received a normal record from upstream and sub-records were later extracted by
  #    the application.  the application should have assigned sequence numbers with sub
  #    and total fields populated when informing us of disposition.  once all such items
  #    are done, we can consider the token associated with the original parent record as
  #    done.
  defp update_pending({state, completed_sequence_numbers}) do
    completed_sequence_numbers
    |> Enum.reduce(state, &update_pending_1/2)
  end

  defp update_pending_1(sn, %__MODULE__{pending: pending, done: done} = state) do
    case Map.pop(pending, sn) do
      {nil, pending} ->
        # the sequence number doesn't exist in pending.  this will happen if the sequence
        # number has sub and total fields populated and a non-aggregate record was
        # received from upstream.  that non-aggregate record's sequence number (lacking
        # sub/total fields) was used as the key, and the value will be {token, [..]}.
        sub = Exmld.sequence_number(sn, :user_sub)
        total = Exmld.sequence_number(sn, :user_total)
        if :undefined == sub do
          exit({:missing_pending, sn})
        end
        key = Exmld.sequence_number(sn, user_sub: :undefined, user_total: :undefined)
        {{token, seen}, pending} = Map.pop(pending, key)
        seen = [{sub, total} | seen]
        # if all expected items have been received, move token to done.  otherwise,
        # continue building seen list;
        case all_done(seen) do
          true ->
            %{state | pending: pending, done: [maybe_wrap_token(token, sn) | done]}
          false ->
            %{state | pending: Map.put(pending, key, {token, seen})}
        end
      {token, pending} ->
        %{state | pending: pending, done: [maybe_wrap_token(token, sn) | done]}
    end
  end

  defp all_done([]) do
    false
  end
  defp all_done([{_sub, total} | _] = values) do
    case length(values) do
      ^total ->
        # every item must have the same total value, and each sub must be unique and cover
        # the range 0..total-1.
        expected = MapSet.new(0..(total-1))
        actual = MapSet.new(Enum.map(values, &(elem(&1, 0))))
        if MapSet.disjoint?(expected, actual) do
          exit({:unexpected_disjoint, expected, actual})
        end
        true
      _ ->
        false
    end
  end

  # while awaiting pending items, we spew heartbeats to all downstream stages so we can
  # obtain disposition of prior items.  if this happens frequently, the downstream
  # pipeline can't keep up with the producer, so its parameters should be tuned.
  defp await_pending(%__MODULE__{await_sleep_interval: sleep_interval,
                                 pending: pending} = state) when map_size(pending) > 0 do
    Logger.info("#{state.shard_id} awaiting #{inspect map_size(pending)} items...")
    :timer.sleep(sleep_interval)
    state
    |> incr(:heartbeats, 1)
    |> notify_downstream(:heartbeat)
    |> update_pending()
    |> await_pending()
  end
  defp await_pending(state) do
    state
  end

  # notify a downstream processing stage of a record or heartbeat and handle any returned
  # item dispositions, returning an updated state and a list of processed sequence
  # numbers.
  #
  # if processing of an item has failed and we aren't configured to skip failed records,
  # we crash.  otherwise we call any configured error callback and skip the failed items.
  defp notify_downstream(%__MODULE__{shard_id: shard_id,
                                     opaque: opaque,
                                     stages: stages} = state, :heartbeat) do
    {state, dispositions} =
      stages
      |> Enum.reduce({0, {:disposition, []}},
                     fn (stage, {n, x}) ->
                       datum = %Datum{shard_id: shard_id,
                                      opaque: opaque,
                                      stream_record: {:heartbeat, {state.counter, state.heartbeats, n}}}
                       {:disposition, y} = Exmld.KinesisStage.notify(stage, datum)
                       {n + 1, put_elem(x, 1, y ++ elem(x, 1))}
                     end)
      |> elem(1)
      |> handle_errors(state)

    {state, Enum.map(dispositions, &(&1.sequence_number))}
  end
  defp notify_downstream(%__MODULE__{shard_id: shard_id,
                                     opaque: opaque} = state, thing) do
    {state, dispositions} =
      choose_stage(state)
      |> Exmld.KinesisStage.notify(%Datum{shard_id: shard_id,
                                          opaque: opaque,
                                          stream_record: thing})
      |> handle_errors(state)

    # we either had no errors, exited, or called a configured error callback for an error.
    # at this point, consider all items as successfully processed and return their
    # sequence numbers:
    {state, Enum.map(dispositions, &(&1.sequence_number))}
  end

  defp choose_stage(%__MODULE__{stages: stages, heartbeats: heartbeats, counter: count}) do
    Enum.at(stages, rem(count + heartbeats, length(stages)))
  end

  defp handle_errors({:disposition, prior_dispositions},
                     %__MODULE__{shard_id: shard_id,
                                 opaque: opaque,
                                 error_callback: cb,
                                 skip_errors: skip_errors} = state) do
    failed = prior_dispositions
    |> Enum.reject(fn (%Disposition{status: status}) ->
                     status == :ok
                   end)

    result = {incr(state, :errors, length(failed)), prior_dispositions}

    case failed do
      [] ->
        result
      _ ->
        if cb do
          cb.(state, failed)
        end

        case skip_errors do
          true ->
            result
          _ ->
            exit({:processing_failed,
                  shard_id: shard_id, opaque: opaque, failures: failed})
        end
    end
  end

  defp log_errors(_state, failed) do
    Logger.error("processing failed: #{inspect failed}")
  end

  defp incr(state, field, n) do
    Map.put(state, field, n + Map.fetch!(state, field))
  end
end
