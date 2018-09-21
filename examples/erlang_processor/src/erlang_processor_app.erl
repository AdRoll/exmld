-module(erlang_processor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% note: in a real application many of these values would be in a common app env
    %% configuration, but are shown here for illustration.  A key `asdf_xyz` appearing in
    %% a producer config map will be used to substitute the variable "${ASDF_XYZ}"
    %% appearing in erlmld/priv/mld.properties.in.
    ErlMldConfig = maps:from_list(application:get_all_env(erlmld)),

    %% emit kcl spam to console.  in a real application, this could be configured using a
    %% lager-compatible module for logging to a file.
    application:set_env(erlmld, log_kcl_spam, true),

    %% id of this worker instance; it should be unique per beam node.  if not supplied, it
    %% will be auto-generated by the KCL.  two different nodes using the same worker id
    %% will clobber each other's state.
    WorkerId = <<"example worker">>,

    %% name and region of the kinesis stream being processed.  you could create this stream
    %% with the following command:
    %%
    %%  aws kinesis create-stream --region us-west-2 --shard-count 2 \
    %%                            --stream-name erlang-processor-test-stream
    %%
    StreamName = <<"erlang-processor-test-stream">>,
    StreamRegion = <<"us-west-2">>,

    %% ARN and region of the dynamo stream being processed.  `erlmld` does not yet support
    %% obtaining ARNs from table names.  you can obtain the ARN of an existing table
    %% stream with the following command:
    %%
    %%  aws dynamodbstreams list-streams --region us-west-2 \
    %%                      --table-name erlang-processor-test-table \
    %%                      --query 'Streams[0].StreamArn' --output text
    %%
    TableStreamArn = <<"arn:aws:dynamodb:REGION:ACCOUNT-ID:table/TABLE-NAME/stream/TIMESTAMP">>,
    TableRegion = StreamRegion,

    %% in this example application, all source streams can be processed the same way, so
    %% we set up a single flow and set of stages.  if data from different streams should
    %% be handled differently, separate flows should be used.
    %%
    %% these are the registered names of the GenStages which will receive kinesis records
    %% from each owned shard (round-robin).  the actual stages will be owned by a
    %% supervisor, but the names are needed now due to how the flusher module and flow are
    %% configured.  they could also be pids, but using names allows them to be restarted
    %% without restarting everything else (and be started later):
    StageNames = [binary_to_atom(<<"stage_", (integer_to_binary(I))/binary>>,
                                 utf8)
                  || I <- lists:seq(1, erlang:system_info(schedulers_online))],

    ConcurrencyFactor = 1, % increase if processing work is IO-bound
    NumReducers = erlang:system_info(schedulers_online) * ConcurrencyFactor,

    %% size of each batch to be "flushed" (i.e., collect this many items before processing
    %% them all in a batch):
    BatchSize = 10,

    %% attempt to flush batches every 10s even if batch size not reached (relies on
    %% heartbeat mechanic):
    FlushInterval = 10000,

    %% checkpoint every 60s:
    CheckpointInterval = 60000,

    %% fail if a worker stalls for 600s:
    WatchdogTimeout = 600000,

    %% max number of in-flight items for each kinesis shard worker:
    MaxPending = 1024,

    %% flow demand parameters; see flow documentation:
    MinDemand = 1,
    MaxDemand = 1024,

    FlowOptions = [{num_stages, NumReducers},
                   {min_demand, MinDemand},
                   {max_demand, MaxDemand}],
    FlowSpec = erlang_processor:flow_spec(StageNames, BatchSize, FlushInterval, FlowOptions),

    %% retrieve this many records with each api call (max: 10000 (kinesis), 1000
    %% (dynamo)):
    MaxRecords = 1000,

    CommonConfig = maps:merge(
                     ErlMldConfig,
                     #{
                       record_processor => erlmld_batch_processor,
                       record_processor_data =>
                           #{flusher_mod => 'Elixir.Exmld.KinesisWorker',
                             flusher_mod_data =>
                                 [{stages, StageNames},
                                  {opaque, some_opaque_value},
                                  {max_pending, MaxPending}],
                             flush_interval_ms => FlushInterval,
                             checkpoint_interval_ms => CheckpointInterval,
                             watchdog_timeout_ms => WatchdogTimeout,
                             on_checkpoint => fun on_checkpoint/2,
                             description => "description goes here"},

                       worker_id => WorkerId,

                       %% initial starting position if no shard checkpoint exists; LATEST is
                       %% most recent, TRIM_HORIZON is earliest available:
                       initial_position => <<"TRIM_HORIZON">>,

                       max_records => MaxRecords,

                       %% reduce cloudwatch metric spam:
                       metrics_level => <<"NONE">>
                      }),

    %% a kinesis stream processor:
    KinesisProducer = #{
      %% required if processing multiple source streams within a single beam node (any
      %% atom, used as a registered name suffix and local filename component):
      app_suffix => k,

      %% this name will be used to name the dynamodb state table used by the KCL.  if
      %% it doesn't exist, it will be created.  the table is used for coordinating
      %% leases held and checkpoints made by workers cooperating as part of an
      %% application.  if two erlang nodes are running using the same value for this
      %% name, they are considered as two workers in a single processing application.
      %% a single beam node processing multiple different streams needs a unique value
      %% for each stream:
      kcl_appname => <<"erlang-processor-kinesis-test">>,

      stream_name => StreamName,
      stream_region => StreamRegion,

      %% the stream type; 'kinesis' for kinesis streams, 'dynamo' for dynamodb
      %% streams:
      stream_type => kinesis
     },

    %% a dynamo stream processor:
    DynamoProducer = #{
      app_suffix => d,
      kcl_appname => <<"erlang-processor-dynamo-test">>,
      stream_name => TableStreamArn,
      stream_region => TableRegion,
      stream_type => dynamo
     },

    ProducerConfigs = [maps:merge(CommonConfig, ProducerConfig)
                       || ProducerConfig <- [KinesisProducer]],%, DynamoProducer]],
    erlang_processor_sup:start_link(#{stage_names => StageNames,
                                      flow_spec => FlowSpec,
                                      producers => ProducerConfigs}).

stop(_State) ->
    ok.


on_checkpoint(OpaqueData, ShardId) ->
    io:format("~p checkpointed (~p)~n", [OpaqueData, ShardId]).
