defmodule Monsoon.BTree.Select do
  alias Monsoon.Log

  def select_from_tree(lower, upper) do
    db = self()

    Stream.resource(
      fn ->
        send(db, {:latest_info, self()})

        {log, leaf_links_loc} =
          receive do
            {:latest_info, {{_, leaf_links_loc, _}, log}} ->
              {log, leaf_links_loc}
          after
            5000 ->
              raise "Failed to receive latest info from server."
          end

        {:ok, leaf_links} = Log.get_leaf_links(log, leaf_links_loc)

        {first, _} =
          Enum.find(leaf_links, fn
            {_id, {nil, _next}} -> true
            _ -> false
          end)

        to_list(first, lower, upper, leaf_links, log)
      end,
      fn
        [] -> {:halt, []}
        [h | t] -> {[h], t}
      end,
      fn _ -> :ok end
    )
  end

  defp to_list(nil, _lower, _upper, _leaf_lins, _log), do: []

  defp to_list(id, lower, upper, leaf_links, log) do
    {_prev, next} = Map.get(leaf_links, id)
    {:ok, leaf} = Log.get_node_by_id(log, id)

    case take_in_range(leaf.keys, leaf.values, lower, upper, []) do
      {:cont, res} ->
        res ++ to_list(next, lower, upper, leaf_links, log)

      {:halt, res} ->
        res
    end
  end

  defp take_in_range(keys, values, nil, nil, _acc) do
    {:cont, Enum.zip(keys, values)}
  end

  defp take_in_range([], [], _lower, _upper, acc), do: {:cont, Enum.reverse(acc)}

  defp take_in_range([k | ks], [v | vs], lower, upper, acc) do
    cond do
      k < lower ->
        take_in_range(ks, vs, lower, upper, acc)

      lower <= k and k <= upper ->
        take_in_range(ks, vs, lower, upper, [{k, v} | acc])

      true ->
        {:halt, Enum.reverse(acc)}
    end
  end
end
