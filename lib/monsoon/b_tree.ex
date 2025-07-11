defmodule Monsoon.BTree do
  @moduledoc """

  """
  alias Monsoon.Log
  alias __MODULE__.{KeyInsert, Search, Select, Copy, KeyDelete}

  defstruct [
    :capacity,
    is_leaf: true,
    pairs: [],
    children: []
  ]

  @type location :: non_neg_integer()
  @type pair :: {term(), term()}
  @type t :: %__MODULE__{
          capacity: non_neg_integer(),
          is_leaf: boolean(),
          pairs: [pair()],
          children: [location()]
        }

  @spec new(log :: Log.t(), capacity :: non_neg_integer()) :: {:ok, location()} | {:error, term()}
  def new(log, capacity) do
    case Log.latest_root_location(log) do
      {:ok, nil} ->
        root = %__MODULE__{capacity: capacity}
        {:ok, root_loc} = Log.put_node(log, root)
        :ok = Log.commit(log, root_loc)
        {:ok, root_loc}

      {:ok, root_loc} ->
        {:ok, root_loc}

      {:error, _reason} = e ->
        e
    end
  end

  @spec copy(from :: Log.t(), to :: Log.t(), root_loc :: location()) ::
          {:ok, location()} | {:error, term()}
  def copy(from, to, root_loc) do
    {:ok, root} = Log.get_node(from, root_loc)
    Copy.copy_tree(from, to, root)
  end

  @spec search(log :: Log.t(), root_loc :: location(), key :: term()) ::
          {:ok, value :: term()} | {:error, term()}
  def search(log, root_loc, key) do
    {:ok, root} = Log.get_node(log, root_loc)
    Search.search_key(log, root, key)
  end

  @spec select(log :: Log.t(), root_loc :: location(), lower :: term(), upper :: term()) ::
          {:ok, list()} | {:error, term()}
  def select(log, root_loc, lower, upper) do
    {:ok, root} = Log.get_node(log, root_loc)
    Select.select_from_tree(log, root, lower, upper)
  end

  @spec insert(log :: Log.t(), root_loc :: location(), key :: term(), value :: term()) ::
          {:ok, root_loc :: location()} | {:error, term()}
  def insert(log, root_loc, key, value) do
    {:ok, root} = Log.get_node(log, root_loc)

    case KeyInsert.kv_insert(log, root, key, value) do
      {:normal, root} ->
        Log.put_node(log, root)

      {:split, {left, split_pair, right}} ->
        with {:ok, left_loc} <- Log.put_node(log, left),
             {:ok, right_loc} <- Log.put_node(log, right) do
          Log.put_node(
            log,
            %__MODULE__{
              capacity: root.capacity,
              is_leaf: false,
              pairs: [split_pair],
              children: [left_loc, right_loc]
            }
          )
        end

      {:error, _reason} = e ->
        e
    end
  end

  def remove(log, root_loc, key) do
    {:ok, root} = Log.get_node(log, root_loc)

    case KeyDelete.kv_delete(log, root, key) do
      nil ->
        # key not found in tree
        {:ok, root_loc}

      {:normal, root} ->
        Log.put_node(log, root)

      {:underflow, %{pairs: [], children: [root_loc]}, _} ->
        {:ok, root_loc}

      {:underflow, root, _} ->
        Log.put_node(log, root)
    end
  end

  @spec debug_print(log :: Log.t(), root_loc :: location()) :: :ok
  def debug_print(log, root_loc) do
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

  defp do_debug_print(log, node, indent, level) do
    node_type = if node.is_leaf, do: "LEAF", else: "INTERNAL"

    pairs_str =
      node.pairs |> Enum.map(fn {k, v} -> "#{inspect(k)}:#{inspect(v)}" end) |> Enum.join(", ")

    IO.puts("#{indent}[#{node_type}] {#{pairs_str}}")

    if not node.is_leaf do
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
end
