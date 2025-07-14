defmodule Monsoon.BTree.Search do
  alias Monsoon.BTree.{Leaf, Interior, Util}
  alias Monsoon.Log

  def search_key(%Leaf{} = node, key, _log) do
    find_kv(node.keys, node.values, key)
  end

  def search_key(%Interior{} = node, key, log) do
    child_idx = Util.find_child_index(node.keys, key)
    child_loc = Enum.at(node.children, child_idx)
    {:ok, child} = Log.get_node(log, child_loc)
    search_key(child, key, log)
  end

  defp find_kv([], [], _key), do: nil
  defp find_kv([k | _ks], [_v | _vs], key) when key < k, do: nil
  defp find_kv([k | ks], [_v | vs], key) when key > k, do: find_kv(ks, vs, key)
  defp find_kv([k | _ks], [v | _vs], key) when k == key, do: v
  # defp to_list(nil, _lower, _upper, _log), do: []
  # defp to_list(%Leaf{} = node, nil, nil, _log), do: Enum.zip(node.keys, node.values)
  #
  # defp to_list(%Leaf{} = node, lower, upper, _log) do
  #   Enum.zip(node.keys, node.values)
  #   |> Enum.filter(fn {k, _v} -> k >= lower and k <= upper end)
  # end
  #
  # defp to_list(%Interior{} = node, lower, upper, log) do
  #   zip(node.children, node.keys, lower, upper)
  #   |> Enum.flat_map(fn {child_pos, pair} ->
  #     child =
  #       case Log.get_node(log, child_pos) do
  #         {:ok, child} ->
  #           child
  #
  #         {:error, reason} ->
  #           raise "Failed to get node, reason: #{inspect(reason)}."
  #       end
  #
  #     child_elements = to_list(log, child, lower, upper)
  #     if pair, do: child_elements ++ [pair], else: child_elements
  #   end)
  # end
  #
  # defp zip(l1, l2, nil, nil) do
  #   Enum.zip(l1, l2 ++ [nil])
  # end
  #
  # defp zip(l1, l2, lower, upper) do
  #   zip_range(l1, l2, lower, upper, [])
  #   |> Enum.reverse()
  # end
  #
  # defp zip_range([last], [], _lower, _upper, acc), do: [{last, nil} | acc]
  #
  # defp zip_range([el1 | l1], [{k, v} | l2], lower, upper, acc) do
  #   cond do
  #     k > upper ->
  #       acc
  #
  #     k < lower ->
  #       zip_range(l1, l2, lower, upper, acc)
  #
  #     true ->
  #       zip_range(l1, l2, lower, upper, [{el1, {k, v}} | acc])
  #   end
  # end
end
