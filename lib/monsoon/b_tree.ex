defmodule Monsoon.BTree do
  @moduledoc """

  """
  alias Monsoon.Log
  alias __MODULE__.{Add, Search, Select, Copy, Remove}

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
            children: [Monsoon.Log.block_pointer()]
          }
  end

  @type t :: Leaf.t() | Interior.t()
  @type btree :: {Log.block_pointer(), Log.block_pointer(), Log.block_pointer()}

  @spec new(log :: Log.t(), capacity :: non_neg_integer) :: {:ok, btree()} | {:error, term()}
  def new(log, capacity) do
    case Log.get_commit(log) do
      {:error, :eof} ->
        root = %{Leaf.new() | capacity: capacity}
        leaf_ptrs = %{root.id => {nil, nil}}
        metadata = []
        root_bp = Log.put_node(log, root)
        leaf_links_bp = Log.put_leaf_links(log, leaf_ptrs)
        metadata_bp = Log.put_metadata(log, metadata)
        btree = {root_bp, leaf_links_bp, metadata_bp}
        :ok = Log.flush(log)
        :ok = Log.commit(log, btree)
        {:ok, btree}

      {:ok, {_root_bp, _leaf_links_bp, _metadata_bp} = btree} ->
        {:ok, btree}

      {:error, _reason} = e ->
        e
    end
  end

  @spec copy(btree(), from :: Log.t(), to :: Log.t()) :: {:ok, btree()}
  def copy(btree, from, to) do
    Copy.copy(btree, from, to)
  end

  def search(btree, log, key) do
    {root_loc, _, _} = btree
    {:ok, root} = Log.get_node(log, root_loc)
    Search.search_key(root, key, log)
  end

  def select(_btree, _log, lower, upper) do
    Select.select_from_tree(lower, upper)
  end

  def add(btree, log, k, v) do
    {root_bp, _, _} = btree
    {:ok, root} = Log.get_node(log, root_bp)

    case Add.add(root, k, v, %{log: log, btree: btree}) do
      {:normal, root, %{btree: {_, leaf_links_bp, metadata_loc}}} ->
        root_bp = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_bp, leaf_links_bp, metadata_loc}}

      {:split, {lchild, split_k, rchild}, %{btree: {_, leaf_links_bp, metadata_loc}}} ->
        lchild_bp = Log.put_node(log, lchild)
        rchild_bp = Log.put_node(log, rchild)

        root = %Interior{
          capacity: root.capacity,
          keys: [split_k],
          children: [lchild_bp, rchild_bp]
        }

        root_bp = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_bp, leaf_links_bp, metadata_loc}}
    end
  end

  def remove(btree, log, key) do
    {root_bp, _, _} = btree
    {:ok, root} = Log.get_node(log, root_bp)

    case Remove.remove(root, key, %{log: log, btree: btree}) do
      nil ->
        {:ok, btree}

      {:normal, root, %{btree: {_, leafs_link_bp, metadata_bp}}} ->
        root_bp = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_bp, leafs_link_bp, metadata_bp}}

      {:underflow, %Interior{keys: [], children: [root_bp]},
       %{btree: {_, leafs_link_bp, metadata_bp}}} ->
        :ok = Log.flush(log)
        {:ok, {root_bp, leafs_link_bp, metadata_bp}}

      {:underflow, root, %{btree: {_, leafs_link_bp, metadata_bp}}} ->
        root_bp = Log.put_node(log, root)
        :ok = Log.flush(log)
        {:ok, {root_bp, leafs_link_bp, metadata_bp}}
    end
  end

  @spec put_metadata(btree(), Log.t(), keyword()) :: btree()
  def put_metadata({root_bp, leaf_links_bp, _metadata_bp}, log, metadata) do
    metadata_bp = Log.put_metadata(log, metadata)
    {root_bp, leaf_links_bp, metadata_bp}
  end

  @spec get_metadata(btree(), Log.t()) :: {:ok, keyword()} | {:error, term()}
  def get_metadata({_root_bp, _leaf_links_bp, metadata_bp}, log) do
    Log.get_metadata(log, metadata_bp)
  end

  # @spec debug_print(log :: Log.t(), root_loc :: location()) :: :ok
  # def debug_print(btree, log) do
  #   {root_loc, _, _} = btree
  #
  #   case Log.get_node(log, root_loc) do
  #     {:ok, root} ->
  #       IO.puts("B-Tree Structure (capacity: #{root.capacity}):")
  #       do_debug_print(log, root, "", 0)
  #       :ok
  #
  #     {:error, reason} ->
  #       IO.puts("Error fetching root node: #{inspect(reason)}")
  #       :error
  #   end
  # end
  #
  # defp do_debug_print(_log, %Leaf{} = node, indent, _level) do
  #   pairs_str =
  #     Enum.zip(node.keys, node.values)
  #     |> Enum.map(fn {k, v} -> "#{inspect(k)}:#{inspect(v)}" end)
  #     |> Enum.join(", ")
  #
  #   IO.puts("#{indent}[LEAF] {#{pairs_str}}")
  # end
  #
  # defp do_debug_print(log, %Interior{} = node, indent, level) do
  #   keys_str = node.keys |> Enum.map(&inspect/1) |> Enum.join(", ")
  #   IO.puts("#{indent}[INTERIOR] keys: [#{keys_str}]")
  #
  #   node.children
  #   |> Enum.with_index()
  #   |> Enum.each(fn {child_loc, index} ->
  #     case Log.get_node(log, child_loc) do
  #       {:ok, child_node} ->
  #         child_indent = indent <> "  "
  #         IO.puts("#{indent}├─ Child #{index}:")
  #         do_debug_print(log, child_node, child_indent, level + 1)
  #
  #       {:error, reason} ->
  #         IO.puts("#{indent}├─ Child #{index}: ERROR - #{inspect(reason)}")
  #     end
  #   end)
  # end
end
