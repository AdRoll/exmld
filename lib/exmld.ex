defmodule Exmld do
  @moduledoc ~S"""
  This allows items extracted from Kinesis stream records (or sub-records in a [KPL
  aggregate record](https://github.com/AdRoll/erlmld/blob/master/proto/kpl_agg.proto)) to
  be processed by a pipeline of workers which may differ in number from the number of
  shards owned by the current node (which is the normal processing model offered by
  [erlmld](https://github.com/AdRoll/erlmld)).

  This is beneficial when using aggregate records which can be processed in approximate
  order according to their partition keys as opposed to strict ordering based on the
  shards they arrived on.  For example, suppose the following two Kinesis records are
  received on two different shards:

      Record 1 (a KPL aggregate record)
        - partition key: "xyzzy"
        - subrecord a:
          - partition key: "asdf"
          - value: "12345"
        - subrecord b:
          - partition key: "fdsa"
          - value: "54321"

      Record 2 (a KPL aggregate record)
        - partition key: "qwer"
        - subrecord a:
          - partition key: "asdf"
          - value: "23456"
        - subrecord b:
          - partition key: "z"
          - value: "0"


  Using the normal Kinesis processing paradigm, each shard will be processed in order.
  `erlmld` supports this by spawning a process for each owned shard, which handles each
  record seen on the shard in sequence:

      Worker 1:
        1. handle record "xyzzy"
          a. handle sub-record "asdf"
          b. handle sub-record "fdsa"

      Worker 2:
        1. handle record "qwer"
          a. handle sub-record "asdf"
          b. handle sub-record "z"


  This can fail to make use of all available resources since the maximum concurrency is
  limited by the number of owned shards.  If the application can tolerate the handling of
  sub-records in a non-strict order, it can use a `Flow`-based MapReduce-style scheme:

      [Worker 1]  [Worker 2]     (processes which produce Kinesis records)
          |           |
          v           v
      [Exmld.KinesisStage, ...]  (stages receiving Exmld.KinesisWorker.Datums)
                |
                v
          [M1] .... [Mn]  (mappers which extract items)
            |\       /|
            | \     / |
            |  \   /  |
            |   \ /   |
            |    \    |
            |   / \   |
            |  /   \  |
            | /     \ |
            |/       \|
          [R1] .... [Rn]  (reducers which handle extracted items)

  The number of reducers is configurable and defaults to the number of schedulers online.
  The processing application will specify a means of extracting a partition key from each
  extracted item; these will be used to consistently map items to reducers (which is where
  the actual application work occurs).

  Using the above example and specifying a sub-record's partition key as an item key:

    1. Worker 1 will produce the "asdf" and "fdsa" sub-records from outer record "xyzzy"
    and send them to a pre-configured `Exmld.KinesisStage` (or round-robin to a list of
    such stages).

    2. Worker 2 will similarly produce the "asdf" and "z" sub-records from outer record
    "qwer".

    3. Each receiving stage will wrap and forward these sub-records for handling by the
    flow.

    4. The application will have provided an "identity" item extraction function since KPL
    aggregation is being used here (or otherwise a function accepting one record and
    returning a list containing a single item).

    5. The application will have provided a partition key extraction function which
    returns an appropriate partition key to be used in consistently mapping items to
    reducers.

    6. The first received "asdf" sub-record is provided to some reducer `Rx`.  The second
    received "asdf" sub-record is provided to the same reducer since its extracted key has
    the same hash.

    7. The "fdsa" and "z" sub-records are similarly provided to some worker `Ry` and/or
    `Rz` based on the hash of their partition keys.

    8. The application-provided reducer function notifies each originating stage of the
    disposition of processing for items received from it as processing progresses.

    9. Eventually, processing disposition is provided back to the originating workers,
    which can decide whether or not (and where) to checkpoint.

  """

  require Record
  Record.defrecord(:sequence_number, Record.extract(:sequence_number,
                                                    from_lib: "erlmld/include/erlmld.hrl"))
  Record.defrecord(:checkpoint, Record.extract(:checkpoint,
                                               from_lib: "erlmld/include/erlmld.hrl"))
  Record.defrecord(:stream_record, Record.extract(:stream_record,
                                                  from_lib: "erlmld/include/erlmld.hrl"))

  @type sequence_number :: record(:sequence_number)
  @type checkpoint :: record(:checkpoint)
  @type stream_record :: record(:stream_record)
  @type shard_id :: binary

  @type item :: any
  @type partition_key :: any
  @type reducer_state :: any

  @doc """
  Accepts a flow producing `Exmld.KinesisWorker.Datum`s (e.g,. a flow created from
  `Exmld.KinesisStage`s) and returns another flow.
  """
  # each stream record should be associated with the genstage which received it and the
  # worker which produced it.  each item extracted from a stream record should indicate
  # the record it came from, the item id within the record, and the total number of items
  # in the record.  the extraction and processing functions should correctly handle
  # heartbeats.  the processing function should process as much data as possible, and
  # periodically inform the source genstages of all the item ids which have been
  # (successfully or not) processed.  those genstages in turn will maintain information
  # about what has been successfully processed, which the producing kinesis workers can
  # use when checkpointing.
  @spec flow(# a flow which produces `Datum`s:
             flow :: Flow.t,
             # arity-1 function mapping a datum to list of zero or more items:
             extract_items_fn :: ((Exmld.KinesisWorker.Datum) -> [item]),
             # arity-1 function or flow partition key shortcut for partitioning items:
             partition_key :: {:elem, non_neg_integer}
                              | {:key, atom}
                              | ((item) -> partition_key),
             # arity-0 function returning initial reducer state:
             state0 :: (() -> reducer_state),
             # arity-2 function accepting item being processed and reducer state:
             process_fn :: ((item, reducer_state) -> reducer_state),
             opts :: keyword) :: Flow.t
  def flow(flow,
           extract_items_fn,
           partition_key,
           state0,
           process_fn,
           opts \\ []) do
    flow
    |> Flow.flat_map(extract_items_fn)
    |> Flow.partition(key: partition_key,
                      stages: opts[:num_stages] || System.schedulers_online(),
                      min_demand: opts[:min_demand] || 1,
                      max_demand: opts[:max_demand] || 500,
                      window: opts[:window] || Flow.Window.global())
    |> Flow.reduce(state0, process_fn)
  end

  @doc """
  You can use this one to keep building your flow after calling flow/6 above.
  """
  def from_stages(opts) do
    Flow.from_stages(opts.stages)
    |> flow(opts.extract_items_fn, opts.partition_key,
            opts.state0, opts.process_fn, opts.flow_opts)
  end

  def start_link(opts) do
    from_stages(opts) |> Flow.start_link()
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end
end
