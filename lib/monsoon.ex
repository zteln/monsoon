defmodule Monsoon do
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
    :tx_btree,
    :tx_holder,
    :log,
    gen: 0
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :capacity, :gen_limit])
    GenServer.start_link(__MODULE__, args, name: opts[:name] || __MODULE__)
  end

  @spec start_transaction(GenServer.server()) :: :ok
  def start_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:start_transaction, self()})
  end

  @spec end_transaction(GenServer.server()) :: :ok
  def end_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:end_transaction, self()})
  end

  @spec cancel_transaction(GenServer.server()) :: :ok
  def cancel_transaction(server \\ __MODULE__) do
    GenServer.call(server, {:cancel_transaction, self()})
  end

  @spec put(GenServer.server(), k :: term(), v :: term()) :: :ok
  def put(server \\ __MODULE__, k, v) do
    GenServer.call(server, {:put, k, v, self()})
  end

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

  def debug(server \\ __MODULE__) do
    GenServer.cast(server, :debug)
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
  def handle_call({:start_transaction, _caller}, _from, %{tx_holder: {_, _}} = state) do
    {:reply, {:error, :transaction_already_started}, state}
  end

  def handle_call({:start_transaction, caller}, _from, state) do
    ref = Process.monitor(caller)

    {:reply, :ok,
     %{
       state
       | tx_btree: state.btree,
         tx_holder: {ref, caller}
     }}
  end

  def handle_call(
        {:end_transaction, caller},
        _from,
        %{tx_holder: {ref, caller}} = state
      ) do
    true = Process.demonitor(ref)

    state =
      %{
        state
        | btree: state.tx_btree,
          tx_btree: nil,
          tx_holder: nil
      }
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call({:end_transaction, _caller}, _from, state) do
    {:reply, {:error, :not_transaction_holder}, state}
  end

  def handle_call(
        {:cancel_transaction, caller},
        _from,
        %{tx_holder: {ref, caller}} = state
      ) do
    true = Process.demonitor(ref)

    state = %{
      state
      | tx_btree: nil,
        tx_holder: nil
    }

    {:reply, :ok, state}
  end

  def handle_call({:cancel_transaction, _caller}, _from, state) do
    {:reply, {:error, :not_transaction_holder}, state}
  end

  def handle_call({:put, k, v, caller}, _from, state) do
    state =
      if match?({_, ^caller}, state.tx_holder) do
        {:ok, tx_btree} = BTree.add(state.tx_btree, state.log, k, v)

        %{state | tx_btree: tx_btree}
      else
        {:ok, btree} = BTree.add(state.btree, state.log, k, v)

        %{state | btree: btree}
      end
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call({:remove, k, caller}, _from, state) do
    state =
      if match?({_, ^caller}, state.tx_holder) do
        {:ok, tx_btree} = BTree.remove(state.tx_btree, state.log, k)

        %{state | tx_btree: tx_btree}
      else
        {:ok, btree} = BTree.remove(state.btree, state.log, k)

        %{state | btree: btree}
      end
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call({:get, k, caller}, _from, state) do
    res =
      if match?({_, ^caller}, state.tx_holder) do
        BTree.search(state.tx_btree, state.log, k)
      else
        BTree.search(state.btree, state.log, k)
      end

    {:reply, res, state}
  end

  def handle_call({:select, lower, upper}, _from, state) do
    {:reply, BTree.select(state.btree, state.log, lower, upper), state}
  end

  @impl GenServer
  def handle_cast(:debug, state) do
    BTree.debug_print(state.btree, state.log)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:latest_info, pid}, state) do
    if match?({_, ^pid}, state.tx_holder) do
      send(pid, {:latest_info, {state.tx_btree, state.log}})
    else
      send(pid, {:latest_info, {state.btree, state.log}})
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _object, _reason}, state) do
    state = %{state | tx_holder: nil, tx_btree: nil}
    {:noreply, state}
  end

  defp commit(%{tx_holder: {_, _}} = state), do: %{state | gen: state.gen + 1}

  defp commit(state) do
    :ok = Log.commit(state.log, state.btree)

    if state.gen > state.gen_limit do
      {:ok, tmp_log} = Log.new(Path.join(state.dir, @db_tmp_file_name))
      {:ok, btree} = BTree.copy(state.btree, state.log, tmp_log)
      new_log = Log.move(state.log, tmp_log)
      %{state | gen: 0, log: new_log, btree: btree}
    else
      %{state | gen: state.gen + 1}
    end
  end
end
