%%
%%  record processor example implementation.
%%

-module(erlang_processor).

-export([flow_spec/4]).

-record(state, {batch_size,
                next_flush_time = os:timestamp(),
                flush_interval = 10000,
                pending_items = []}).

-record(flow_token, {stage, worker, sequence_number}).

-record(item, {value, token}).

-include_lib("erlmld/include/erlmld.hrl").


%% return a flow spec which can be used to set up a processing pipeline; see exmld.ex.
flow_spec(StageNames, BatchSize, FlushInterval, FlowOptions) ->
    #{stages => StageNames,
      extract_items_fn => fun flow_extract/1,
      partition_key => {elem, 0}, % elixir uses 0-indexing
      state0 => fun () ->
                        #state{flush_interval = FlushInterval,
                               batch_size = BatchSize}
                end,
      process_fn => fun flow_process_event/2,
      flow_opts => FlowOptions}.


%% flow_extract/1 is called to extract sub-items from a kinesis or dynamo stream record.
%% this allows handling of both KPL-aggregated records and custom aggregation schemes.
%% the output of this function should be a list of 2-tuples ({key, value}) to be passed to
%% flow_process_event/2 for processing in a reducer.
%%
%% items seen by the extract function generally look like this:
%%
%% #{'__struct__' => 'Elixir.Exmld.KinesisStage.Event',
%%   event =>
%%     #{'__struct__' => 'Elixir.Exmld.KinesisWorker.Datum',
%%       opaque => {<<"us-west-2">>, <<"erlang-processor-kinesis-test">>},
%%       shard_id => <<"shardId-000000000001">>,
%%       stream_record =>
%%         #stream_record{partition_key = <<"12345">>,
%%                        timestamp = 946684800,,
%%                        sequence_number =
%%                          #sequence_number{base = 12345, sub = 0},
%%                        data = << .. record data .. >>}},
%%   stage => <0.136.0>,
%%   worker => <0.862.0>}
%%
flow_extract(#{'__struct__' := 'Elixir.Exmld.KinesisStage.Event',
               event := #{'__struct__' := 'Elixir.Exmld.KinesisWorker.Datum',
                          stream_record := {heartbeat, X}}}) ->
    %% handle a heartbeat.  the second element of the tuple will vary so heartbeats get
    %% distributed among reducers, so the elements must be swapped since we're using the
    %% first element as a partition key.
    [{X, heartbeat}];
flow_extract(#{'__struct__' := 'Elixir.Exmld.KinesisStage.Event',
               stage := Stage,
               worker := Worker,
               event := Item}) ->
    %% in a real application, sub-records could be extracted from Event here.  if using a
    %% custom non-KPL aggregation scheme, this should associate each sub-record with a
    %% faked sequence number having the same base as the parent record, and appropriate
    %% 'user_sub' (sub-record index) and 'user_total' (total number of extracted
    %% sub-records) fields.  then when later notifying exmld of record disposition, it can
    %% properly track sub-record processing and advance the checkpoint beyond the parent
    %% record if all of its sub-records were processed.
    %%
    %% two records having the same key (first tuple element) here will be handled by the
    %% same reducer.  in general, the key should be consistently derived from some
    %% attribute of the record/item being processed.
    #{'__struct__' := 'Elixir.Exmld.KinesisWorker.Datum',
      stream_record := #stream_record{sequence_number = SN}} = Item,
    [{erlang:phash2(Item), #item{value = Item,
                                 token = #flow_token{stage = Stage,
                                                     worker = Worker,
                                                     sequence_number = SN#sequence_number{user_sub = 0,
                                                                                          user_total = 1}}}}].


%% process an item extracted from a record (or a heartbeat).  this occurs in a reducer
%% whose initial state is given by 'state0' in flow_spec/4 above.  it returns an updated
%% state after processing the event (and possibly flushing/updating the state
%% accordingly).  here, we simply add the item to the current batch and possibly flush the
%% batch.
flow_process_event({_Key, Item}, #state{} = State) ->
    maybe_flush(flow_add_record(Item, State)).


flow_add_record(heartbeat, State) ->
    State;
flow_add_record(Item, #state{pending_items = Pending} = State) ->
    State#state{pending_items = [Item | Pending]}.


%% possibly process the current pending batch of records if of the appropriate size or
%% enough time has elapsed:
maybe_flush(State) ->
    case should_flush(State) of
        true ->
            {ok, NState, Tokens} = flush(State),
            ok = notify_dispositions(Tokens, ok),
            note_flush(NState);
        false ->
            State
    end.


should_flush(#state{pending_items = Pending,
                    batch_size = BatchSize,
                    next_flush_time = NextFlush}) ->
    length(Pending) >= BatchSize
        orelse elapsed_ms(NextFlush) >= 0.


note_flush(#state{flush_interval = FlushInterval} = State) ->
    {Mega, Sec, Micros} = os:timestamp(),
    NextFlush = {Mega, Sec, Micros + trunc(FlushInterval * 1.0e3)},
    State#state{next_flush_time = NextFlush}.


elapsed_ms(When) ->
    trunc(timer:now_diff(os:timestamp(), When)/1.0e3).


%% process a batch of items which have been collected, returning {ok, NState, Tokens}.
%%
%% Tokens is a list of tokens used by notify_dispositions/2 to inform upstream workers of
%% the status of processing.  this is needed because a single reducer will potentially
%% receive records from multiple different kinesis shards.  with this disposition scheme,
%% a kinesis worker can correctly checkpoint based on how far along downstream processing
%% has come (instead of for example automatically checkpointing based on time, which could
%% lose records).
flush(#state{pending_items = Pending} = State) ->
    io:format("~p processing items: ~p~n", [self(), Pending]),
    timer:sleep(100 * length(Pending)),
    Tokens = [Item#item.token || Item <- Pending],
    {ok, State#state{pending_items = []}, Tokens}.


%% group item processing disposition by origin stage and worker, informing each stage of
%% the records (sequence numbers) from its workers which have been processed.  this allows
%% upstream kinesis workers to safely checkpoint only fully processed data.
notify_dispositions(Tokens, Status) ->
    RecipientMap =
        lists:foldl(
          fun (#flow_token{stage = Stage,
                           worker = Worker,
                           sequence_number = SN}, Acc) ->
                  This = disposition(SN, Status),
                  maps:update_with(
                    Stage,
                    fun (WAcc) ->
                            maps:update_with(
                              Worker,
                              fun (DAcc) ->
                                      [This | DAcc]
                              end,
                              [This],
                              WAcc)
                    end,
                    #{Worker => [This]},
                    Acc)
          end, #{}, Tokens),
    maps:fold(fun (Stage, WorkerMap, ok) ->
                      'Elixir.Exmld.KinesisStage':disposition(Stage, WorkerMap)
              end, ok, RecipientMap).


disposition(SN, Status) ->
    #{'__struct__' => 'Elixir.Exmld.KinesisWorker.Disposition',
      sequence_number => SN,
      status => Status}.
