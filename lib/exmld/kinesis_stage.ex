defmodule Exmld.KinesisStage do
  use GenStage
  require Logger

  @moduledoc """
  A `GenStage` stage for use in processing data produced by `Exmld.KinesisWorker`s.

  This module acts as a GenStage producer.  Subscribers will receive
  `Exmld.KinesisStage.Event`s, which each wrap an underlying `Exmld.KinesisWorker.Datum`
  along with information about the producing worker and stage.  Downstream processors
  should eventually call `disposition/2` with the disposition of processing so that
  originating workers can checkpoint.

  The expected use and workflow is:

  1. Create a stage or set of stages using this module for each distinct processing
  pipeline.  A set of stages could be used by more than one Kinesis stream if the
  processing being done is the same for all of them.

  2. Create a flow using `Flow.from_stages/2`.

  3. Configure the flow using `Exmld.flow/6`.

  4. Run the flow, which should run forever.

  5. Configure an erlmld supervision tree with a set of `Exmld.KinesisWorker`s using the
  stage(s) created in (1).
  """

  defstruct [{:counter, 0},
             {:queue, :queue.new()},
             {:demand, 0},
             {:disposition, %{}}] # pid => {ref, [term]}

  defmodule Event do
    @doc """
    Struct for events provided to an `Exmld.KinesisStage`.

    Records the stage and worker identifiers associated with an event.

    ## Fields

      * `:stage`  - identifier of the `Exmld.KinesisStage` which handled the event
      * `:worker` - identifier of the `Exmld.KinesisWorker` which produced the event
      * `:event`  - an `Exmld.KinesisWorker.Datum`
    """
    defstruct stage: nil, worker: nil, event: nil
    @type t :: %Event{stage: pid, worker: term, event: Exmld.KinesisWorker.Datum.t}
  end

  def start_link(opts \\ []) do
    GenStage.start_link(__MODULE__, [], opts)
  end

  @doc """
  Notify `stage` of a new Kinesis record available for processing.

  A new event is available for processing by `stage`.  The caller will be monitored and
  associated with the new event, and will be blocked until after the event has been used
  to satisfy some downstream demand.  The return value will be the disposition
  (success/failure) of zero or more records which were previously processed.
  """
  @spec notify(GenStage.stage,
               Exmld.KinesisWorker.Datum,
               :infinity | non_neg_integer) :: {:disposition, [Exmld.KinesisWorker.Disposition.t]}
  def notify(stage, datum, timeout \\ :infinity) do
    GenStage.call(stage, {:notify, datum}, timeout)
  end

  @doc """
  Notify `stage` of the disposition of processing some items.

  An attempt has been made to process some data extracted from a Kinesis record by a
  downstream processor.  `stage` will look up the originating producer and record the
  disposition of processing in the next batch of data to be returned to that producer.
  """
  @spec disposition(GenStage.stage, %{optional(pid) => [Exmld.KinesisWorker.Disposition.t]}) :: :ok
  def disposition(stage, disposition, timeout \\ :infinity) do
    GenStage.call(stage, {:disposition, disposition}, timeout)
  end

  ## GenStage callbacks

  def init([]) do
    {:producer, %__MODULE__{}}
  end

  # retrieve any buffered events from state and try to serve any pending demand. associate
  # each event with the process which produced it.  defer reply and block the caller until
  # the event is used to fulfill some demand.  monitor caller.
  def handle_call({:notify, event}, from, state) do
    state
    |> monitor_sender(from)
    |> enqueue_event(event, from)
    |> dispatch_events([])
  end

  # a record processor stage is informing us of the disposition of some items which were
  # extracted from a source record.  forward that information back to the originating
  # workers by updating the next reply state for each pid if it's still monitored.
  def handle_call({:disposition, worker_values}, _from,
                  %__MODULE__{counter: counter, disposition: disposition} = state) do
    # worker_values: %{worker_pid => [disposition]}
    update = fn({worker_pid, worker_dispositions}, map) ->
               map
               |> Map.get_and_update(worker_pid,
                                     fn
                                       (nil) ->
                                         {nil, nil}
                                       ({mref, x}) ->
                                         {nil, {mref, worker_dispositions ++ x}}
                                     end)
               |> elem(1)
             end
    disposition = Enum.reduce(worker_values, disposition, update)
    counter = Enum.reduce(worker_values, counter, fn ({_, d}, n) -> n + length(d) end)
    {:reply, :ok, [], %{state | counter: counter, disposition: disposition}}
  end

  # a monitored process has exited; discard any saved item disposition:
  def handle_info({:DOWN, mref, :process, pid, _reason},
                  %__MODULE__{disposition: disposition} = state) do
    {{^mref, _}, disposition} = Map.pop(disposition, pid)
    {:noreply, [], %{state | disposition: disposition}}
  end

  @doc """
  Handle subscriber demand.

  Return up to `incoming_demand + pending_demand` events, fetching (from state) as needed,
  and storing in state any excess.  If not enough events are available, record unsatisfied
  demand in state, and then return those events when answering a subsequent call.  See the
  `QueueBroadcaster` example in `GenStage` for an explanation of this demand queueing
  behavior.
  """
  def handle_demand(incoming_demand, %__MODULE__{demand: pending_demand} = state) do
    dispatch_events(%{state | demand: incoming_demand + pending_demand}, [])
  end

  ## Internal functions

  defp monitor_sender(%__MODULE__{disposition: disposition} = state, {pid, _}) do
    case Map.has_key?(disposition, pid) do
      true ->
        state
      false ->
        mref = Process.monitor(pid)
        %{state | disposition: Map.put(disposition, pid, {mref, []})}
    end
  end

  defp enqueue_event(%__MODULE__{queue: queue} = state, event, from) do
    %{state | queue: :queue.in({from, event}, queue)}
  end

  defp dispatch_events(%__MODULE__{demand: 0} = state, events) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(%__MODULE__{queue: queue, demand: demand} = state, events) do
    case :queue.out(queue) do
      {{:value, {from, event}}, queue} ->
        {pid, _} = from
        %{state | queue: queue, demand: demand - 1}
        |> inform_of_disposition(from)
        |> dispatch_events([%__MODULE__.Event{stage: self(), worker: pid, event: event} | events])

      {:empty, queue} ->
        {:noreply, Enum.reverse(events), %{state | queue: queue}}
    end
  end

  # look up the pid and send it the next disposition batch as a reply to its latest
  # notification.
  defp inform_of_disposition(%__MODULE__{disposition: disposition} = state,
                             {pid, _ref} = from) do
    {value, disposition} =
      disposition
      |> Map.get_and_update(pid,
                            fn
                              (nil) ->
                                {[], nil}
                              ({mref, x}) ->
                                {x, {mref, []}}
                            end)
    :ok = GenStage.reply(from, {:disposition, value})
    %{state | disposition: disposition}
  end
end
