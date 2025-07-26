defmodule Monsoon do
  @moduledoc """
  Copy-on-Write B+Tree (inherent MVCC).
  Last write wins.
  Single writer.
  """
  use GenServer
  alias Monsoon.BTree

  @default_capacity 16
  @default_gen_limit 2

  defstruct [
    :dir,
    :gen_limit,
    :capacity,
    :btree,
    :tx,
    write_queue: :queue.new(),
    gen: 0
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :capacity, :gen_limit])
    GenServer.start_link(__MODULE__, args, name: opts[:name] || __MODULE__)
  end

  @spec transaction(GenServer.server(), f :: fun(), timeout :: timeout()) ::
          :ok | {:error, :tx_failed}
  def transaction(server \\ __MODULE__, f, timeout \\ :infinity) do
    GenServer.call(server, {:transaction, f, self()}, timeout)
  end

  @spec put_metadata(GenServer.server(), metadata :: keyword(), timeout()) :: :ok
  def put_metadata(server \\ __MODULE__, metadata, timeout \\ :infinity) do
    GenServer.call(server, {:put_metadata, metadata, self()}, timeout)
  end

  @spec get_metadata(GenServer.server(), timeout()) :: keyword()
  def get_metadata(server \\ __MODULE__, timeout \\ :infinity) do
    GenServer.call(server, {:get_metadata, self()}, timeout)
  end

  @spec put(GenServer.server(), k :: term(), v :: term(), timeout()) :: :ok
  def put(server \\ __MODULE__, k, v, timeout \\ :infinity) do
    GenServer.call(server, {:put, k, v, self()}, timeout)
  end

  @spec remove(GenServer.server(), k :: term(), timeout()) :: :ok
  def remove(server \\ __MODULE__, k, timeout \\ :infinity) do
    GenServer.call(server, {:remove, k, self()}, timeout)
  end

  @spec get(GenServer.server(), k :: term(), timeout()) :: {:ok, term()} | {:error, nil | term()}
  def get(server \\ __MODULE__, k, timeout \\ :infinity) do
    GenServer.call(server, {:get, k, self()}, timeout)
  end

  # @spec select(GenServer.server(), timeout()) :: Stream.t()
  # def select(server \\ __MODULE__, timeout \\ 5000) do
  #   GenServer.call(server, {:select, nil, nil}, timeout)
  # end

  @spec select(GenServer.server(), lower :: term(), upper :: term(), timeout()) :: Stream.t()
  def select(server \\ __MODULE__, lower \\ nil, upper \\ nil, timeout \\ :infinity) do
    GenServer.call(server, {:select, lower, upper}, timeout)
  end

  @impl GenServer
  def init(args) do
    dir = args[:dir] || raise "no dir provided."
    capacity = args[:capacity] || @default_capacity
    gen_limit = args[:gen_limit] || @default_gen_limit

    case BTree.new(dir, capacity * 2) do
      {:ok, btree} ->
        {:ok,
         %__MODULE__{
           dir: dir,
           btree: btree,
           capacity: 2 * capacity,
           gen_limit: gen_limit
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:transaction, f, _caller}, from, state) do
    case state.tx do
      nil ->
        task = op(:tx, f)
        state = %{state | tx: {state.btree, task, from}}
        {:noreply, state}

      _ ->
        write_queue = :queue.in({:tx, f, from}, state.write_queue)
        state = %{state | write_queue: write_queue}
        {:noreply, state}
    end
  end

  def handle_call({:put, k, v, caller}, from, state) do
    case state.tx do
      nil ->
        btree = op(:put, {state.btree, {k, v}})
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}

      {btree, %{pid: ^caller} = task, from} ->
        btree = op(:put, {btree, {k, v}})
        state = %{state | tx: {btree, task, from}}
        {:reply, :ok, state, {:continue, :commit}}

      _ ->
        write_queue = :queue.in({:put, {k, v}, from}, state.write_queue)
        state = %{state | write_queue: write_queue}
        {:noreply, state}
    end
  end

  def handle_call({:remove, k, caller}, from, state) do
    case state.tx do
      nil ->
        btree = op(:remove, {state.btree, k})
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}

      {btree, %{pid: ^caller} = task, from} ->
        btree = op(:remove, {btree, k})
        state = %{state | tx: {btree, task, from}}
        {:reply, :ok, state, {:continue, :commit}}

      _ ->
        write_queue = :queue.in({:remove, k, from}, state.write_queue)
        state = %{state | write_queue: write_queue}
        {:noreply, state}
    end
  end

  def handle_call({:get, k, caller}, _from, state) do
    res =
      case state.tx do
        {btree, %{pid: ^caller}, _from} ->
          op(:get, {btree, k})

        _ ->
          op(:get, {state.btree, k})
      end

    {:reply, res, state}
  end

  def handle_call({:select, lower, upper}, _from, state) do
    me = self()

    info_f = fn ->
      send(me, {:latest_info, self()})

      receive do
        {:latest_info, btree} ->
          btree
      after
        5000 ->
          raise "Failed to receive latest info from server."
      end
    end

    reply = op(:select, {info_f, lower, upper})
    {:reply, reply, state}
  end

  def handle_call({:put_metadata, metadata, caller}, from, state) do
    case state.tx do
      nil ->
        btree = op(:put_metadata, {state.btree, metadata})
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}

      {btree, %{pid: ^caller} = task, from} ->
        btree = op(:put_metadata, {btree, metadata})
        state = %{state | tx: {btree, task, from}}
        {:reply, :ok, state, {:continue, :commit}}

      _ ->
        write_queue = :queue.in({:put_metadata, metadata, from}, state.write_queue)
        state = %{state | write_queue: write_queue}
        {:noreply, state}
    end
  end

  def handle_call({:get_metadata, caller}, _from, state) do
    res =
      case state.tx do
        {btree, %{pid: ^caller}, _from} ->
          op(:get_metadata, btree)

        _ ->
          op(:get_metadata, state.btree)
      end

    {:reply, res, state}
  end

  @impl GenServer
  def handle_info({:latest_info, pid}, state) do
    case state.tx do
      {btree, %{pid: ^pid}, _from} ->
        send(pid, {:latest_info, btree})

      _ ->
        send(pid, {:latest_info, state.btree})
    end

    {:noreply, state}
  end

  def handle_info({_ref, :done}, state) do
    {btree, _task, from} = state.tx
    GenServer.reply(from, :ok)
    state = %{state | tx: nil, btree: btree}
    {:noreply, state, {:continue, :commit}}
  end

  def handle_info({_ref, :cancel}, state) do
    {_btree, _task, from} = state.tx
    GenServer.reply(from, :ok)
    state = %{state | tx: nil}
    {:noreply, state, {:continue, :dequeue}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case state.tx do
      {_btree, %{pid: ^pid}, from} ->
        GenServer.reply(from, {:error, :tx_failed})
        state = %{state | tx: nil}
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:commit, state) do
    {:noreply, commit(state), {:continue, :dequeue}}
  end

  def handle_continue(:dequeue, state) do
    state = dequeue(state)
    {:noreply, state}
  end

  defp dequeue(state) do
    case :queue.out(state.write_queue) do
      {:empty, write_queue} ->
        %{state | write_queue: write_queue}

      {{:value, {:tx, f, from}}, write_queue} ->
        task = op(:tx, f)
        tx = {state.btree, task, from}
        GenServer.reply(from, :ok)
        %{state | tx: tx, write_queue: write_queue}

      {{:value, {:put, {k, v}, from}}, write_queue} ->
        btree = op(:put, {state.btree, {k, v}})
        GenServer.reply(from, :ok)

        %{state | btree: btree, write_queue: write_queue}
        |> commit()
        |> dequeue()

      {{:value, {:remove, k, from}}, write_queue} ->
        btree = op(:remove, {state.btree, k})
        GenServer.reply(from, :ok)

        %{state | btree: btree, write_queue: write_queue}
        |> commit()
        |> dequeue()

      {{:value, {:put_metadata, metadata, from}}, write_queue} ->
        btree = op(:put_metadata, {state.btree, metadata})
        GenServer.reply(from, :ok)

        %{state | btree: btree, write_queue: write_queue}
        |> commit()
        |> dequeue()
    end
  end

  defp op(code, arg) do
    case code do
      :tx ->
        Task.async(arg)

      :put ->
        {btree, {k, v}} = arg
        BTree.add(btree, k, v)

      :remove ->
        {btree, k} = arg
        BTree.remove(btree, k)

      :get ->
        {btree, k} = arg
        BTree.search(btree, k)

      :select ->
        {info_f, lower, upper} = arg
        BTree.select(info_f, lower, upper)

      :put_metadata ->
        {btree, metadata} = arg
        BTree.put_metadata(btree, metadata)

      :get_metadata ->
        BTree.get_metadata(arg)
    end
  end

  defp commit(state) do
    case state.tx do
      nil ->
        :ok = BTree.commit(state.btree)
        %{state | gen: state.gen + 1} |> maybe_vacuum()

      _ ->
        %{state | gen: state.gen + 1}
    end
  end

  defp maybe_vacuum(state) do
    if state.gen > state.gen_limit do
      btree = BTree.copy(state.btree, state.dir)
      %{state | gen: 0, btree: btree}
    else
      state
    end
  end
end
