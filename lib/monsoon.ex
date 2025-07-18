defmodule Monsoon do
  @moduledoc """
  Copy-on-Write B+Tree.
  Last write wins.
  """
  use GenServer
  alias Monsoon.BTree
  alias Monsoon.Log

  @db_file_name "db.monsoon"
  @db_tmp_file_name "tmp.monsoon"

  defstruct [
    :dir,
    :gen_limit,
    :capacity,
    :btree,
    :log,
    gen: 0,
    tx_holders: %{}
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :capacity, :gen_limit])
    GenServer.start_link(__MODULE__, args, name: opts[:name] || __MODULE__)
  end

  @doc """
  Starts a transaction that is process-bound. 
  The changes during a transaction are only committed once a transaction ends.

  ## Examples
      
      :ok = Monsoon.start_transaction(db)
  """
  @spec start_transaction(GenServer.server()) :: :ok
  def start_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:start_transaction, self()})
  end

  @doc """
  Ends a started transaction. This will commit the changes to the log.

  ## Examples

    :ok = Monsoon.start_transaction(db)
    # ...
    :ok = Monsoon.end_transaction(db)
  """
  @spec end_transaction(GenServer.server()) :: :ok
  def end_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:end_transaction, self()})
  end

  @doc """
  Cancels a started transaction. 
  The changes made during the transaction are not committed.

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
    GenServer.call(server, {:put_metadata, metadata})
  end

  @spec get_metadata(GenServer.server()) :: keyword()
  def get_metadata(server \\ __MODULE__) do
    GenServer.call(server, :get_metadata)
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
    capacity = args[:capacity] || 16
    gen_limit = args[:gen_limit] || 10

    with {:ok, log} <- Log.new(Path.join(dir, @db_file_name)),
         {:ok, btree} <- BTree.new(log, capacity * 2) do
      {:ok,
       %__MODULE__{
         dir: dir,
         log: log,
         btree: btree,
         capacity: capacity,
         gen_limit: gen_limit
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:start_transaction, caller}, _from, state) do
    {reply, state} =
      case Map.get(state.tx_holders, caller) do
        nil ->
          ref = Process.monitor(caller)
          tx_holders = Map.put(state.tx_holders, caller, {state.btree, ref})
          state = %{state | tx_holders: tx_holders}
          {:ok, state}

        _ ->
          {{:error, :transaction_already_started}, state}
      end

    {:reply, reply, state}
  end

  def handle_call({:end_transaction, caller}, _from, state) do
    {reply, state} =
      case Map.get(state.tx_holders, caller) do
        nil ->
          {{:error, :no_transaction}, state}

        {tx_btree, ref} ->
          true = Process.demonitor(ref)
          tx_holders = Map.delete(state.tx_holders, caller)
          state = %{state | btree: tx_btree, tx_holders: tx_holders} |> commit(caller)
          {:ok, state}
      end

    {:reply, reply, state}
  end

  def handle_call({:cancel_transaction, caller}, _from, state) do
    {reply, state} =
      case Map.get(state.tx_holders, caller) do
        nil ->
          {{:error, :not_in_transaction}, state}

        {_, ref} ->
          true = Process.demonitor(ref)
          tx_holders = Map.delete(state.tx_holders, caller)
          state = %{state | tx_holders: tx_holders}
          {:ok, state}
      end

    {:reply, reply, state}
  end

  def handle_call({:put, k, v, caller}, _from, state) do
    state =
      case Map.get(state.tx_holders, caller) do
        nil ->
          {:ok, btree} = BTree.add(state.btree, state.log, k, v)
          %{state | btree: btree}

        {btree, ref} ->
          {:ok, btree} = BTree.add(btree, state.log, k, v)
          tx_holders = Map.put(state.tx_holders, caller, {btree, ref})
          %{state | tx_holders: tx_holders}
      end
      |> commit(caller)

    {:reply, :ok, state}
  end

  def handle_call({:remove, k, caller}, _from, state) do
    state =
      case Map.get(state.tx_holders, caller) do
        nil ->
          {:ok, btree} = BTree.remove(state.btree, state.log, k)
          %{state | btree: btree}

        {btree, ref} ->
          {:ok, btree} = BTree.remove(btree, state.log, k)
          tx_holders = Map.put(state.tx_holders, caller, {btree, ref})
          %{state | tx_holders: tx_holders}
      end
      |> commit(caller)

    {:reply, :ok, state}
  end

  def handle_call({:get, k, caller}, _from, state) do
    res =
      case Map.get(state.tx_holders, caller) do
        nil ->
          BTree.search(state.btree, state.log, k)

        {btree, _ref} ->
          BTree.search(btree, state.log, k)
      end

    {:reply, res, state}
  end

  def handle_call({:select, lower, upper}, _from, state) do
    {:reply, BTree.select(state.btree, state.log, lower, upper), state}
  end

  def handle_call({:put_metadata, metadata}, _from, state) do
    btree = BTree.put_metadata(state.btree, state.log, metadata)
    state = %{state | btree: btree}
    {:reply, :ok, state}
  end

  def handle_call(:get_metadata, _from, state) do
    metadata = BTree.get_metadata(state.btree, state.log)
    {:reply, metadata, state}
  end

  @impl GenServer
  def handle_info({:latest_info, pid}, state) do
    case Map.get(state.tx_holders, pid) do
      nil ->
        send(pid, {:latest_info, {state.btree, state.log}})

      {btree, _ref} ->
        send(pid, {:latest_info, {btree, state.log}})
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _object, _reason}, state) do
    tx_holders =
      Enum.filter(state.tx_holders, fn {_pid, {_btree, tx_ref}} ->
        tx_ref == ref
      end)

    state = %{state | tx_holders: tx_holders}
    {:noreply, state}
  end

  defp commit(state, pid) do
    case Map.get(state.tx_holders, pid) do
      nil ->
        :ok = Log.commit(state.log, state.btree)
        %{state | gen: state.gen + 1} |> maybe_vacuum()

      _ ->
        %{state | gen: state.gen + 1}
    end
  end

  defp maybe_vacuum(state) do
    if state.tx_holders == %{} and state.gen > state.gen_limit do
      {:ok, tmp_log} = Log.new(Path.join(state.dir, @db_tmp_file_name))
      {:ok, btree} = BTree.copy(state.btree, state.log, tmp_log)
      new_log = Log.move(state.log, tmp_log)
      %{state | gen: 0, log: new_log, btree: btree}
    else
      state
    end
  end
end
