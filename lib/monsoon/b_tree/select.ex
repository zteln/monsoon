defmodule Monsoon.BTree.Select do
  alias Monsoon.Log

  def select_from_tree(log, node, lower, upper) do
    Stream.resource(
      fn -> {to_list(log, node, lower, upper), 0} end,
      fn
        {[], _} -> {:halt, {[], 0}}
        {[h | t], _} -> {[h], {t, 0}}
      end,
      fn _ -> :ok end
    )
  end

  defp to_list(_log, nil, _lower, _upper), do: []
  defp to_list(_log, %{is_leaf: true} = node, nil, nil), do: node.pairs

  defp to_list(_log, %{is_leaf: true} = node, lower, upper) do
    node.pairs
    |> Enum.filter(fn {k, _v} -> k >= lower and k <= upper end)
  end

  defp to_list(log, node, lower, upper) do
    zip(node.children, node.pairs, lower, upper)
    |> Enum.flat_map(fn {child_pos, pair} ->
      child =
        case Log.get_node(log, child_pos) do
          {:ok, child} ->
            child

          {:error, reason} ->
            raise "Failed to get node, reason: #{inspect(reason)}."
        end

      child_elements = to_list(log, child, lower, upper)
      if pair, do: child_elements ++ [pair], else: child_elements
    end)
  end

  defp zip(l1, l2, nil, nil) do
    Enum.zip(l1, l2 ++ [nil])
  end

  defp zip(l1, l2, lower, upper) do
    zip_range(l1, l2, lower, upper, [])
    |> Enum.reverse()
  end

  defp zip_range([last], [], _lower, _upper, acc), do: [{last, nil} | acc]

  defp zip_range([el1 | l1], [{k, v} | l2], lower, upper, acc) do
    cond do
      k > upper ->
        acc

      k < lower ->
        zip_range(l1, l2, lower, upper, acc)

      true ->
        zip_range(l1, l2, lower, upper, [{el1, {k, v}} | acc])
    end
  end
end
