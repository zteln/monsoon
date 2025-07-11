defmodule Monsoon do
  use GenServer
  alias Monsoon.BTree
  alias Monsoon.Log

  @db_file_name "0.monsoon"
  @db_tmp_file_name "tmp.monsoon"

  defstruct [
    :dir,
    :gen_limit,
    :capacity,
    :root_cursor,
    :saved_root_cursor,
    :log,
    gen: 0,
    is_in_transaction: false
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :capacity, :gen_limit])
    GenServer.start_link(__MODULE__, args, name: opts[:name] || __MODULE__)
  end

  @spec start_transaction(GenServer.server()) :: :ok
  def start_transaction(server \\ __MODULE__) do
    GenServer.call(server, :start_transaction)
  end

  @spec end_transaction(GenServer.server()) :: :ok
  def end_transaction(server \\ __MODULE__) do
    GenServer.call(server, :end_transaction)
  end

  @spec cancel_transaction(GenServer.server()) :: :ok
  def cancel_transaction(server \\ __MODULE__) do
    GenServer.call(server, :cancel_transaction)
  end

  @spec put(GenServer.server(), k :: term(), v :: term()) :: :ok
  def put(server \\ __MODULE__, k, v) do
    GenServer.call(server, {:put, k, v})
  end

  def remove(server \\ __MODULE__, k) do
    GenServer.call(server, {:remove, k})
  end

  @spec get(GenServer.server(), k :: term()) :: {:ok, term()} | {:error, nil | term()}
  def get(server \\ __MODULE__, k) do
    GenServer.call(server, {:get, k})
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
         {:ok, root_cursor} <- BTree.new(log, capacity * 2) do
      {:ok,
       %__MODULE__{
         dir: dir,
         log: log,
         root_cursor: root_cursor,
         capacity: capacity,
         gen_limit: gen_limit
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(:start_transaction, _from, state) do
    {:reply, :ok, %{state | is_in_transaction: true, saved_root_cursor: state.root_cursor}}
  end

  def handle_call(:end_transaction, _from, state) do
    state =
      %{state | is_in_transaction: false, saved_root_cursor: nil}
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call(:cancel_transaction, _from, state) do
    state = %{
      state
      | is_in_transaction: false,
        root_cursor: state.saved_root_cursor,
        saved_root_cursor: nil
    }

    {:reply, :ok, state}
  end

  def handle_call({:put, k, v}, _from, state) do
    {:ok, root_cursor} = BTree.insert(state.log, state.root_cursor, k, v)

    state =
      %{state | root_cursor: root_cursor}
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call({:remove, k}, _from, state) do
    {:ok, root_cursor} = BTree.remove(state.log, state.root_cursor, k)

    state =
      %{state | root_cursor: root_cursor}
      |> commit()

    {:reply, :ok, state}
  end

  def handle_call({:get, k}, _from, state) do
    {:reply, BTree.search(state.log, state.root_cursor, k), state}
  end

  def handle_call({:select, lower, upper}, _from, state) do
    {:reply, BTree.select(state.log, state.root_cursor, lower, upper), state}
  end

  @impl GenServer
  def handle_cast(:debug, state) do
    BTree.debug_print(state.log, state.root_cursor)
    {:noreply, state}
  end

  defp commit(%{is_in_transaction: true} = state), do: state

  defp commit(state) do
    :ok = Log.commit(state.log, state.root_cursor)

    if state.gen > state.gen_limit do
      {:ok, tmp_log} = Log.new(Path.join(state.dir, @db_tmp_file_name))
      {:ok, root_cursor} = BTree.copy(state.log, tmp_log, state.root_cursor)
      new_log = Log.rename(tmp_log, state.log)
      :ok = Log.stop(state.log)
      %{state | gen: 0, log: new_log, root_cursor: root_cursor}
    else
      %{state | gen: state.gen + 1}
    end
  end
end
