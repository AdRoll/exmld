erlang_processor
=====

An example erlang kinesis record processing application using `exmld`.

Note: running this example as-is will incur new costs in your AWS account of ~$11/mo (two
new dynamodb KCL state tables with default read/write capacity of 10/10).  Change the
capacity of each table to 1/1 to reduce to ~$1.20/mo.

Edit
-----

 Edit the following variables in
 [src/erlang_processor_app.erl](src/erlang_processor_app.erl)
 according to the resources in your account / desired testing:

  1. `StreamName`      - a kinesis stream name
  2. `StreamRegion`    - region of the stream
  3. `TableStreamArn`  - a dynamodb table stream ARN
  4. `TableRegion`     - region of the table stream
  5. `ProducerConfigs` - list of producers to run (e.g., to test only kinesis or dynamo)

Build
-----

    $ make

Run
-----

    $ rebar3 shell
    1> observer:start().

Disable KCL logspam
-----

    2> application:set_env(erlmld, log_kcl_spam, false).
    ok

Observe
-----

    <0.434.0> processing items: [{item,
                              #{'__struct__' =>
                                 'Elixir.Exmld.KinesisWorker.Datum',
                                opaque => some_opaque_value,
                                shard_id =>
                                 <<"shardId-00000001537808865642-00000000">>,
                                stream_record =>
                                 {stream_record,undefined,undefined,
                                  undefined,
                                  {sequence_number,
                                   2027497200000000000664507565,undefined,
                                   undefined,undefined},
                                  <<"{\"eventID\":\"00000000000000000000000000000000\",\"eventName\":\"INSERT\",\"eventVersion\":\"1.1\",\"eventSource\":\"aws:dynamodb\",\"awsRegion\":\"us-west-2\",\"dynamodb\":{\"ApproximateCreationDateTime\":1537821240000,\"Keys\": ... },\"SequenceNumber\":\"00000000000000000000000000000000\",\"SizeBytes\":1234,\"StreamViewType\":\"KEYS_ONLY\"}}">>}},
                              {flow_token,<0.424.0>,<0.459.0>,
                               {sequence_number,00000000000000000000000000000000,
                                undefined,0,1}}},
                                ...


Make a release
-----

    $ make release
    ... (unpack release somewhere) ...
    in release dir:
    $ ./bin/erlang_processor foreground
