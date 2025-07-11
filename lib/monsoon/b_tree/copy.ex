defmodule Monsoon.BTree.Copy do
  alias Monsoon.Log

  @spec copy_tree(from :: Log.t(), to :: Log.t(), root :: Monsoon.BTree.t()) :: :ok
  def copy_tree(from, to, root) do
    {:ok, new_root_loc} = update_children(from, to, root)
    :ok = Log.commit(to, new_root_loc)
    {:ok, new_root_loc}
  end

  defp update_children(_from, to, %{is_leaf: true} = node) do
    Log.put_node(to, node)
  end

  defp update_children(from, to, node) do
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
