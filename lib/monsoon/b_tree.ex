defmodule Monsoon.BTree do
  @moduledoc """

  """
  alias Monsoon.Log
  alias __MODULE__.{Add, Search, Select, Copy, Remove}

  @type location :: non_neg_integer()

  defmodule Leaf do
    defstruct [
      :id,
      :capacity,
      keys: [],
      values: []
    ]

    @type t :: %__MODULE__{
            id: binary(),
            capacity: non_neg_integer(),
            keys: [term()],
            values: [term()]
          }

    def new do
      %__MODULE__{id: gen_id()}
    end

    defp gen_id do
      :crypto.strong_rand_bytes(8)
    end
  end

  defmodule Interior do
    defstruct [
      :capacity,
      keys: [],
      children: []
    ]

    @type t :: %__MODULE__{
            capacity: non_neg_integer(),
            keys: [term()],
            children: [Monsoon.BTree.location()]
          }
  end

  @type t :: Leaf.t() | Interior.t()

  def new(log, capacity) do
    case Log.get_commit(log) do
      {:ok, nil} ->
        root = %{Leaf.new() | capacity: capacity}
        leaf_ptrs = %{root.id => {nil, nil}}
        {:ok, root_loc} = Log.put_node(log, root)
        {:ok, leaf_links_loc} = Log.put_leaf_links(log, leaf_ptrs)
        btree = {root_loc, leaf_links_loc, 0}
        :ok = Log.flush(log)
        :ok = Log.commit(log, btree)
        {:ok, btree}

      {:ok, {_root_loc, _leaf_links_loc, _metadata_loc} = btree} ->
        {:ok, btree}

      {:error, _reason} = e ->
        e
    end
  end

  # @spec copy(from :: Log.t(), to :: Log.t(), root_loc :: location()) ::
  #         {:ok, location()} | {:error, term()}
  def copy(btree, from, to) do
    Copy.copy(btree, from, to)
  end

  # @spec search(log :: Log.t(), root_loc :: location(), key :: term()) ::
  #         {:ok, value :: term()} | {:error, term()}
  def search(btree, log, key) do
    {root_loc, _, _} = btree
    {:ok, root} = Log.get_node(log, root_loc)
    Search.search_key(root, key, log)
  end

  # @spec select(log :: Log.t(), root_loc :: location(), lower :: term(), upper :: term()) ::
  #         {:ok, list()} | {:error, term()}
  def select(_btree, _log, lower, upper) do
    Select.select_from_tree(lower, upper)
  end

  def add(btree, log, k, v) do
    {root_loc, _, _} = btree
    {:ok, root} = Log.get_node(log, root_loc)

    case Add.add(root, k, v, %{log: log, btree: btree}) do
      {:normal, root, %{btree: {_, leaf_links_loc, metadata_loc}}} ->
        {:ok, root_loc} = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_loc, leaf_links_loc, metadata_loc}}

      {:split, {lchild, split_k, rchild}, %{btree: {_, leaf_links_loc, metadata_loc}}} ->
        {:ok, lchild_loc} = Log.put_node(log, lchild)
        {:ok, rchild_loc} = Log.put_node(log, rchild)

        root = %Interior{
          capacity: root.capacity,
          keys: [split_k],
          children: [lchild_loc, rchild_loc]
        }

        {:ok, root_loc} = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_loc, leaf_links_loc, metadata_loc}}
    end
  end

  def remove(btree, log, key) do
    {root_loc, _, _} = btree
    {:ok, root} = Log.get_node(log, root_loc)

    case Remove.remove(root, key, %{log: log, btree: btree}) do
      nil ->
        {:ok, btree}

      {:normal, root, %{btree: {_, leafs_link_loc, metadata_loc}}} ->
        {:ok, root_loc} = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_loc, leafs_link_loc, metadata_loc}}

      {:underflow, %Interior{keys: [], children: [root_loc]},
       %{btree: {_, leafs_link_loc, metadata_loc}}} ->
        :ok = Log.flush(log)
        {:ok, {root_loc, leafs_link_loc, metadata_loc}}

      {:underflow, root, %{btree: {_, leafs_link_loc, metadata_loc}}} ->
        {:ok, root_loc} = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_loc, leafs_link_loc, metadata_loc}}
    end
  end

  @spec debug_print(log :: Log.t(), root_loc :: location()) :: :ok
  def debug_print(btree, log) do
    {root_loc, _, _} = btree

    case Log.get_node(log, root_loc) do
      {:ok, root} ->
        IO.puts("B-Tree Structure (capacity: #{root.capacity}):")
        do_debug_print(log, root, "", 0)
        :ok

      {:error, reason} ->
        IO.puts("Error fetching root node: #{inspect(reason)}")
        :error
    end
  end

  defp do_debug_print(_log, %Leaf{} = node, indent, _level) do
    pairs_str =
      Enum.zip(node.keys, node.values)
      |> Enum.map(fn {k, v} -> "#{inspect(k)}:#{inspect(v)}" end)
      |> Enum.join(", ")

    IO.puts("#{indent}[LEAF] {#{pairs_str}}")
  end

  defp do_debug_print(log, %Interior{} = node, indent, level) do
    keys_str = node.keys |> Enum.map(&inspect/1) |> Enum.join(", ")
    IO.puts("#{indent}[INTERIOR] keys: [#{keys_str}]")

    node.children
    |> Enum.with_index()
    |> Enum.each(fn {child_loc, index} ->
      case Log.get_node(log, child_loc) do
        {:ok, child_node} ->
          child_indent = indent <> "  "
          IO.puts("#{indent}├─ Child #{index}:")
          do_debug_print(log, child_node, child_indent, level + 1)

        {:error, reason} ->
          IO.puts("#{indent}├─ Child #{index}: ERROR - #{inspect(reason)}")
      end
    end)
  end
end
