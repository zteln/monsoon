# defmodule Monsoon.BTree.Copy do
#   alias Monsoon.BTree.{Leaf, Interior}
#   alias Monsoon.Log
#
#   # @spec copy(from :: Log.t(), to :: Log.t(), root :: Monsoon.BTree.t()) :: :ok
#   def copy(btree, from, to) do
#     {root_bp, leaf_links_bp, metadata_bp} = btree
#     root_bp = copy_tree(from, to, root_bp)
#     leaf_links_bp = copy_leaf_links(from, to, leaf_links_bp)
#     btree = {root_bp, leaf_links_bp, metadata_bp}
#     :ok = Log.flush(to)
#     :ok = Log.commit(to, btree)
#     btree
#   end
#
#   defp copy_leaf_links(from, to, leaf_links_bp) do
#     {:ok, leaf_links} = Log.get_leaf_links(from, leaf_links_bp)
#     Log.put_leaf_links(to, leaf_links)
#   end
#
#   defp copy_tree(from, to, root_bp) do
#     {:ok, root} = Log.get_node(from, root_bp)
#     update_children(from, to, root)
#   end
#
#   defp update_children(_from, to, %Leaf{} = node) do
#     Log.put_node(to, node)
#   end
#
#   defp update_children(from, to, %Interior{} = node) do
#     new_children =
#       node.children
#       |> Enum.map(fn child_bp ->
#         {:ok, child} = Log.get_node(from, child_bp)
#         update_children(from, to, child)
#       end)
#
#     node = %{node | children: new_children}
#     Log.put_node(to, node)
#   end
# end
