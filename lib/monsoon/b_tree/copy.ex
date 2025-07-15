defmodule Monsoon.BTree.Copy do
  alias Monsoon.BTree.{Leaf, Interior}
  alias Monsoon.Log

  # @spec copy(from :: Log.t(), to :: Log.t(), root :: Monsoon.BTree.t()) :: :ok
  def copy(btree, from, to) do
    {root_loc, leaf_links_loc, metadata_loc} = btree
    {:ok, root_loc} = copy_tree(from, to, root_loc)
    {:ok, leaf_links_loc} = copy_leaf_links(from, to, leaf_links_loc)
    btree = {root_loc, leaf_links_loc, metadata_loc}
    :ok = Log.flush(to)
    :ok = Log.commit(to, btree)
    {:ok, btree}
  end

  defp copy_leaf_links(from, to, leaf_links_loc) do
    {:ok, leaf_links} = Log.get_leaf_links(from, leaf_links_loc)
    Log.put_leaf_links(to, leaf_links)
  end

  defp copy_tree(from, to, root_loc) do
    {:ok, root} = Log.get_node(from, root_loc)
    update_children(from, to, root)
  end

  defp update_children(_from, to, %Leaf{} = node) do
    Log.put_node(to, node)
  end

  defp update_children(from, to, %Interior{} = node) do
    new_children =
      node.children
      |> Enum.map(fn child_loc ->
        {:ok, child} = Log.get_node(from, child_loc)
        {:ok, new_child_loc} = update_children(from, to, child)
        new_child_loc
      end)

    node = %{node | children: new_children}
    Log.put_node(to, node)
  end
end
