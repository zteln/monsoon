defmodule Monsoon.LeafLinks do
  alias Monsoon.Log

  @doc """
  Splits leaf links during a leaf split in the B-Tree.

  Before:
  ... <-> prev <-> node <-> next <-> ...
  After:
  ... <-> prev <-> lnode <-> rnode <-> next <-> ...
  """
  @spec split(
          leaf_links_bp :: Log.block_pointer(),
          lnode :: Monsoon.BTree.Leaf.t(),
          node :: Monsoon.BTree.Leaf.t(),
          rnode :: Monsoon.BTree.Leaf.t(),
          log :: Log.t()
        ) :: Log.block_point()
  def split(leaf_links_bp, lnode, node, rnode, log) do
    {:ok, leaf_links} = Log.get_leaf_links(log, leaf_links_bp)

    {prev, next} = Map.get(leaf_links, node.id)

    leaf_links =
      case {prev, next} do
        {nil, nil} ->
          leaf_links

        {prev, nil} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)

          leaf_links
          |> Map.put(prev, {prev_prev, lnode.id})

        {nil, next} ->
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(next, {rnode.id, next_next})

        {prev, next} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(prev, {prev_prev, lnode.id})
          |> Map.put(next, {rnode.id, next_next})
      end
      |> Map.delete(node.id)
      |> Map.put(lnode.id, {prev, rnode.id})
      |> Map.put(rnode.id, {lnode.id, next})

    Log.put_leaf_links(log, leaf_links)
  end

  @doc """
  Merges leaf links during a leaf merge.

  Before:
  ... <-> prev <-> lnode <-> rnode <-> next <-> ...
  After
  ... <-> prev <-> mnode <-> next <-> ...
  """
  @spec merge(
          leaf_links_bp :: Monsoon.Log.block_pointer(),
          lnode :: Monsoon.BTree.Leaf.t(),
          mnode :: Monsoon.BTree.Leaf.t(),
          rnode :: Monsoon.BTree.Leaf.t(),
          log :: Monsoon.Log.t()
        ) :: Monsoon.Log.block_point()
  def merge(leaf_links_bp, lnode, mnode, rnode, log) do
    {:ok, leaf_links} = Log.get_leaf_links(log, leaf_links_bp)

    {prev, _next} = Map.get(leaf_links, lnode.id)
    {_prev, next} = Map.get(leaf_links, rnode.id)

    leaf_links =
      case {prev, next} do
        {nil, nil} ->
          leaf_links

        {prev, nil} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)

          leaf_links
          |> Map.put(prev, {prev_prev, mnode.id})

        {nil, next} ->
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(next, {mnode.id, next_next})

        {prev, next} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(prev, {prev_prev, mnode.id})
          |> Map.put(next, {mnode.id, next_next})
      end
      |> Map.delete(lnode.id)
      |> Map.delete(rnode.id)
      |> Map.put(mnode.id, {prev, next})

    Log.put_leaf_links(log, leaf_links)
  end
end
