-module(erlang_processor_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Opts) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Opts).

init(#{stage_names := StageNames,
       flow_spec := FlowSpec,
       producers := ProducerConfigs}) ->

    SupFlags = #{strategy => one_for_all,
                 intensity => 2,
                 period => 10},

    Producers =
        [#{id => {mld_producer, N},
           type => supervisor,
           shutdown => infinity,
           start => {erlmld_sup, start_link, [ProducerConfig]}}
         || {N, ProducerConfig} <- lists:zip(lists:seq(1, length(ProducerConfigs)),
                                             ProducerConfigs)],

    %% the stages must be associated with live processes at the time the flow is started.
    %% if the stages are restarted, the flow should also be restarted.  thus the
    %% one_for_all restart strategy.
    FlowWorker = #{id => flow,
                   type => worker,
                   shutdown => 5000,
                   start => {'Elixir.Exmld', start_link, [FlowSpec]}},

    Stages = [#{id => StageName,
                type => worker,
                shutdown => 5000,
                start => {'Elixir.Exmld.KinesisStage', start_link, [[{name, StageName}]]}}
              || StageName <- StageNames],

    {ok, {SupFlags, Stages ++ [FlowWorker | Producers]}}.
