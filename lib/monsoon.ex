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
    gen: 0
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :capacity, :gen_limit])
    GenServer.start_link(__MODULE__, args, name: opts[:name] || __MODULE__)
  end

  @doc """
  Starts a transaction that is process-bound. 
  The changes during a transaction are only committed once a transaction ends.
  This makes the process occupy the writer roler if no other transaction exists already.
  Starting a transaction will block writes in other processes, though it will not block reads.

  ## Examples
      
      :ok = Monsoon.start_transaction(db)
      spawn(fn -> {:error, :tx_occupied} = Monsoon.start_transaction(db) end)
      spawn(fn -> {:error, :not_tx_proc} = Monsoon.put(db, :key, :value) end)
      {:error, :tx_already_started} = Monsoon.start_transaction(db)
  """
  @spec start_transaction(GenServer.server()) ::
          :ok | {:error, :tx_already_started | :tx_occupied}
  def start_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:start_transaction, self()})
  end

  @doc """
  Ends a started transaction. 
  This will commit the changes made during the transaction and unblock other writers.

  ## Examples

    :ok = Monsoon.start_transaction(db)
    # ...
    :ok = Monsoon.end_transaction(db)
  """
  @spec end_transaction(GenServer.server()) :: :ok | {:error, :not_tx_proc}
  def end_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:end_transaction, self()})
  end

  @doc """
  Cancels a started transaction. 
  The changes made during the transaction are not committed and unblocks other writers.

  ## Examples

      :ok = Monsoon.start_transaction(db)
      # ...
      :ok = Monsoon.end_transaction(db)
  """
  @spec cancel_transaction(GenServer.server()) :: :ok
  def cancel_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:cancel_transaction, self()})
  end

  @spec put_metadata(GenServer.server(), metadata :: keyword()) :: :ok
  def put_metadata(server \\ __MODULE__, metadata) do
    GenServer.call(server, {:put_metadata, metadata, self()})
  end

  @spec get_metadata(GenServer.server()) :: keyword()
  def get_metadata(server \\ __MODULE__) do
    GenServer.call(server, {:get_metadata, self()})
  end

  @spec put(GenServer.server(), k :: term(), v :: term()) :: :ok
  def put(server \\ __MODULE__, k, v) do
    GenServer.call(server, {:put, k, v, self()})
  end

  @spec remove(GenServer.server(), k :: term()) :: :ok
  def remove(server \\ __MODULE__, k) do
    GenServer.call(server, {:remove, k, self()})
  end

  @spec get(GenServer.server(), k :: term()) :: {:ok, term()} | {:error, nil | term()}
  def get(server \\ __MODULE__, k) do
    GenServer.call(server, {:get, k, self()})
  end

  @spec select(GenServer.server()) :: Stream.t()
  def select(server \\ __MODULE__) do
    GenServer.call(server, {:select, nil, nil})
  end

  @spec select(GenServer.server(), lower :: term(), upper :: term()) :: Stream.t()
  def select(server \\ __MODULE__, lower, upper) do
    GenServer.call(server, {:select, lower, upper})
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
  def handle_call({:start_transaction, caller}, _from, state) do
    case state.tx do
      {^caller, _tx_btree, _ref} ->
        {:reply, {:error, :tx_already_started}, state}

      {_tx_holder, _tx_btree, _ref} ->
        {:reply, {:error, :tx_occupied}, state}

      nil ->
        ref = Process.monitor(caller)
        tx = {caller, state.btree, ref}
        state = %{state | tx: tx}
        {:reply, :ok, state}
    end
  end

  def handle_call({:end_transaction, caller}, _from, state) do
    case state.tx do
      {^caller, tx_btree, ref} ->
        true = Process.demonitor(ref)
        state = %{state | btree: tx_btree, tx: nil}
        {:reply, :ok, state, {:continue, :commit}}

      _ ->
        {:reply, {:error, :not_tx_proc}, state}
    end
  end

  def handle_call({:cancel_transaction, caller}, _from, state) do
    case state.tx do
      {^caller, _tx_btree, ref} ->
        true = Process.demonitor(ref)
        state = %{state | tx: nil}
        {:reply, :ok, state}

      _ ->
        {:reply, {:error, :not_tx_proc}, state}
    end
  end

  def handle_call({:put, k, v, caller}, _from, state) do
    case state.tx do
      nil ->
        btree = BTree.add(state.btree, k, v)
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}

      {^caller, tx_btree, ref} ->
        tx_btree = BTree.add(tx_btree, k, v)
        tx = {caller, tx_btree, ref}
        state = %{state | tx: tx}
        {:reply, :ok, state, {:continue, :commit}}

      {_pid, _, _} ->
        {:reply, {:error, :not_tx_proc}, state}
    end
  end

  def handle_call({:remove, k, caller}, _from, state) do
    case state.tx do
      nil ->
        btree = BTree.remove(state.btree, k)
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}

      {^caller, tx_btree, ref} ->
        tx_btree = BTree.remove(tx_btree, k)
        tx = {caller, tx_btree, ref}
        state = %{state | tx: tx}
        {:reply, :ok, state, {:continue, :commit}}

      {_pid, _, _} ->
        {:reply, {:error, :not_tx_proc}, state}
    end
  end

  def handle_call({:get, k, caller}, _from, state) do
    res =
      case state.tx do
        {^caller, tx_btree, _ref} ->
          BTree.search(tx_btree, k)

        _ ->
          BTree.search(state.btree, k)
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

    {:reply, BTree.select(info_f, lower, upper), state}
  end

  def handle_call({:put_metadata, metadata, caller}, _from, state) do
    case state.tx do
      {^caller, tx_btree, ref} ->
        tx_btree = BTree.put_metadata(tx_btree, metadata)
        state = %{state | tx: {caller, tx_btree, ref}}
        {:reply, :ok, state, {:continue, :commit}}

      {_pid, _, _} ->
        {:reply, {:error, :not_tx_proc}, state}

      nil ->
        btree = BTree.put_metadata(state.btree, metadata)
        state = %{state | btree: btree}
        {:reply, :ok, state, {:continue, :commit}}
    end

    btree = BTree.put_metadata(state.btree, metadata)
    state = %{state | btree: btree}
    {:reply, :ok, state}
  end

  def handle_call({:get_metadata, caller}, _from, state) do
    res =
      case state.tx do
        {^caller, tx_btree, _ref} ->
          BTree.get_metadata(tx_btree)

        _ ->
          BTree.get_metadata(state.btree)
      end

    {:reply, res, state}
  end

  @impl GenServer
  def handle_info({:latest_info, pid}, state) do
    case state.tx do
      {^pid, tx_btree, _ref} ->
        send(pid, {:latest_info, tx_btree})

      _ ->
        send(pid, {:latest_info, state.btree})
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    case state.tx do
      {_pid, _tx_btree, ^ref} ->
        state = %{state | tx: nil}
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:commit, state) do
    {:noreply, commit(state)}
  end

  defp commit(state) do
    case state.tx do
      nil ->
        :ok = BTree.commit(state.btree)
        # :ok = Log.commit(state.log, state.btree)
        %{state | gen: state.gen + 1} |> maybe_vacuum()

      _ ->
        %{state | gen: state.gen + 1}
    end
  end

  defp maybe_vacuum(state) do
    if is_nil(state.tx) and state.gen > state.gen_limit do
      # {:ok, tmp_log} = Log.new(Path.join(state.dir, @db_tmp_file_name))
      btree = BTree.copy(state.btree, state.dir)
      # new_log = Log.move(state.log, tmp_log)
      %{state | gen: 0, btree: btree}
    else
      state
    end
  end
end
