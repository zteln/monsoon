defmodule Monsoon.BTree.KeyInsert do
  alias Monsoon.BTree
  alias Monsoon.Log

  @spec kv_insert(log :: GenServer.server(), node :: BTree.t(), key :: term(), value :: term()) ::
          {:normal, BTree.t()} | {:split, {BTree.t(), {term(), term()}, BTree.t()}}
  def kv_insert(_log, %BTree{is_leaf: true} = node, key, value) do
    case search_index(node.pairs, key, 0) do
      {:exact, idx} ->
        # update key with new value
        pairs = List.replace_at(node.pairs, idx, {key, value})
        node = %{node | pairs: pairs}
        {:normal, node}

      {:next, idx} ->
        insert_and_maybe_split_node(node, idx, key, value)
    end
  end

  def kv_insert(log, node, key, value) do
    case search_index(node.pairs, key, 0) do
      {:exact, idx} ->
        pairs = List.replace_at(node.pairs, idx, {key, value})
        node = %{node | pairs: pairs}
        {:normal, node}

      {:next, idx} ->
        child_pos = Enum.at(node.children, idx)

        {:ok, child} = Log.get_node(log, child_pos)

        case kv_insert(log, child, key, value) do
          {:normal, child} ->
            with {:ok, child_pos} <- Log.put_node(log, child) do
              children = List.replace_at(node.children, idx, child_pos)
              node = %{node | children: children}
              {:normal, node}
            end

          {:split, {left_child, {split_key, split_value}, right_child}} ->
            with {:ok, left_pos} <- Log.put_node(log, left_child),
                 {:ok, right_pos} <- Log.put_node(log, right_child) do
              insert_and_maybe_split_node(
                node,
                idx,
                split_key,
                split_value,
                left_pos,
                right_pos
              )
            end
        end
    end
  end

  defp insert_and_maybe_split_node(
         node,
         idx,
         key,
         value,
         left_pos \\ nil,
         right_pos \\ nil
       ) do
    if length(node.pairs) < node.capacity - 1 do
      # not full
      pairs = List.insert_at(node.pairs, idx, {key, value})

      children =
        if left_pos && right_pos do
          node.children
          |> List.insert_at(idx, left_pos)
          |> List.replace_at(idx + 1, right_pos)
        else
          []
        end

      node = %{node | pairs: pairs, children: children}
      {:normal, node}
    else
      # full
      k = div(node.capacity, 2)

      {left, split_pair, right} =
        cond do
          idx < k ->
            # insert into left half
            split(
              node,
              k,
              &insert_subarray(&1, idx, {key, value}, 0, k - 1),
              &subarray(&1, k, length(&1)),
              left_pos && right_pos && (&split_subarray(&1, idx, left_pos, right_pos, 0, k)),
              left_pos && right_pos && (&subarray(&1, k, length(&1)))
            )

          true ->
            # insert into right half
            split(
              node,
              k,
              &subarray(&1, 0, k - 1),
              &insert_subarray(&1, idx, {key, value}, k, length(&1)),
              left_pos && right_pos && (&subarray(&1, 0, k)),
              left_pos && right_pos &&
                (&split_subarray(&1, idx, left_pos, right_pos, k, length(&1)))
            )
        end

      {:split, {left, split_pair, right}}
    end
  end

  defp split(
         node,
         k,
         left_pairs_f,
         right_pairs_f,
         left_children_f,
         right_children_f
       ) do
    left_pairs = left_pairs_f.(node.pairs)

    left_children =
      if left_children_f do
        left_children_f.(node.children)
      else
        []
      end

    right_pairs = right_pairs_f.(node.pairs)

    right_children =
      if right_children_f do
        right_children_f.(node.children)
      else
        []
      end

    split_pair = Enum.at(node.pairs, k - 1)

    left = %BTree{
      is_leaf: node.is_leaf,
      capacity: node.capacity,
      pairs: left_pairs,
      children: left_children
    }

    right = %BTree{
      is_leaf: node.is_leaf,
      capacity: node.capacity,
      pairs: right_pairs,
      children: right_children
    }

    {left, split_pair, right}
  end

  defp subarray(arr, start, beyond) do
    Enum.slice(arr, start, beyond - start)
  end

  defp insert_subarray(arr, idx, el, start, beyond) do
    arr
    |> subarray(start, beyond)
    |> List.insert_at(idx - start, el)
  end

  defp split_subarray(arr, idx, x, y, start, beyond) do
    new_idx = idx - start + 1

    arr
    |> insert_subarray(idx, x, start, beyond)
    |> List.replace_at(new_idx, y)
  end

  defp search_index([], _key, idx), do: {:next, idx}
  defp search_index([{k, _v} | _pairs], key, idx) when k == key, do: {:exact, idx}

  defp search_index([{k, _v} | pairs], key, idx) when k < key,
    do: search_index(pairs, key, idx + 1)

  defp search_index([{k, _v} | _pairs], key, idx) when k > key, do: {:next, idx}
end
