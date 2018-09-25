# ElixirProcessor

An example elixir kinesis / dynamodb streams processing application using `exmld`.

Note: running this example as-is will incur new costs in your AWS account of ~$11/mo (two
new dynamodb KCL state tables with default read/write capacity of 10/10).  Change the
capacity of each table to 1/1 to reduce to ~$1.20/mo.


## Edit

 Edit the following variables in
 [lib/elixir_processor/application.ex](lib/elixir_processor/application.ex) according to
 the resources in your account / desired testing:

  1. `stream_name`      - a kinesis stream name
  2. `stream_region`    - region of the stream
  3. `table_stream_arn` - a dynamodb table stream ARN
  4. `table_region`     - region of the table stream
  5. `producer_configs` - list of producers to run (e.g., to test only kinesis or dynamo)


## Build

    $ make


## Run

    $ iex -S mix
    iex(1)> :observer.start()


## Disable KCL logspam

    iex(2)> Application.put_env(:erlmld, :log_kcl_spam, false)
    :ok


## Observe

    11:36:45.688 pid=<0.199.0> items=[%ElixirProcessor.Item{token: %ElixirProcessor.Token{sequence_number: {:sequence_number, 00000000000000000000000000000000, :undefined, 0, 1}, stage: #PID<0.189.0>, worker: #PID<0.212.0>}, value: {:stream_record, :undefined, :undefined, :undefined, {:sequence_number, 00000000000000000000000000000000, :undefined, :undefined, :undefined}, "{\"eventID\":\"00000000000000000000000000000000\",\"eventName\":\"REMOVE\",\"eventVersion\":\"1.1\",\"eventSource\":\"aws:dynamodb\",\"awsRegion\":\"us-west-2\",\"dynamodb\": ... event data ... }"}}] line=96 function=flush/1 module=ElixirProcessor file=lib/elixir_processor.ex application=elixir_processor [info]  processing batch
