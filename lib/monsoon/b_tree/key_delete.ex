defmodule Monsoon.BTree.KeyDelete do
  alias Monsoon.BTree
  alias Monsoon.Log

  @spec kv_delete(log :: Log.t(), node :: BTree.t(), key :: term) ::
          nil | {:deleted, BTree.t(), {BTree.pair(), boolean()}}
  def kv_delete(_log, %BTree{is_leaf: true} = node, key) do
    case search_index(node.pairs, key, 0) do
      {:exact, idx} ->
        # key found
        {pair, pairs} = List.pop_at(node.pairs, idx)
        node = %{node | pairs: pairs}

        if length(pairs) <= div(node.capacity, 2) - 1 do
          {:underflow, node, pair}
        else
          {:normal, node}
        end

      _ ->
        # key not found
        nil
    end
  end

  def kv_delete(log, node, key) do
    case search_index(node.pairs, key, 0) do
      {:exact, idx} ->
        # child to the left of match
        child_loc = Enum.at(node.children, idx)
        {:ok, child} = Log.get_node(log, child_loc)

        case remove_last(log, child) do
          {:normal, child, pair} ->
            pairs = List.replace_at(node.pairs, idx, pair)
            {:ok, child_loc} = Log.put_node(log, child)
            children = List.replace_at(node.children, idx, child_loc)
            node = %{node | pairs: pairs, children: children}
            {:normal, node}

          {:underflow, child, pair} ->
            pairs = List.replace_at(node.pairs, idx, pair)
            node = %{node | pairs: pairs}

            if idx < length(node.pairs) do
              rchild_loc = Enum.at(node.children, idx + 1)
              {:ok, rchild} = Log.get_node(log, rchild_loc)
              handle_underflow(log, pair, idx, child, node, rchild, true)
            else
              idx = idx - 1
              lchild_loc = Enum.at(node.children, idx)
              {:ok, lchild} = Log.get_node(log, lchild_loc)
              handle_underflow(log, pair, idx, lchild, node, child, false)
            end
        end

      {:next, idx} ->
        # no match, check closest child
        child_loc = Enum.at(node.children, idx)
        {:ok, child} = Log.get_node(log, child_loc)

        case kv_delete(log, child, key) do
          nil ->
            nil

          {:normal, child} ->
            {:ok, child_loc} = Log.put_node(log, child)
            children = List.replace_at(node.children, idx, child_loc)
            node = %{node | children: children}
            {:normal, node}

          {:underflow, child, pair} ->
            if idx < length(node.pairs) do
              rchild_loc = Enum.at(node.children, idx + 1)
              {:ok, rchild} = Log.get_node(log, rchild_loc)
              handle_underflow(log, pair, idx, child, node, rchild, true)
            else
              idx = idx - 1
              lchild_loc = Enum.at(node.children, idx)
              {:ok, lchild} = Log.get_node(log, lchild_loc)
              handle_underflow(log, pair, idx, lchild, node, child, false)
            end
        end
    end
  end

  defp handle_underflow(
         log,
         pair,
         idx,
         %{is_leaf: true} = lchild,
         parent,
         %{is_leaf: true} = rchild,
         from_left?
       ) do
    is_minimal =
      if from_left?,
        do: is_minimal(rchild.capacity, rchild.pairs),
        else: is_minimal(lchild.capacity, lchild.pairs)

    if not is_minimal do
      # sibling is not minimal, rotate keys
      {lpairs, pairs, rpairs} =
        rotate_keys(idx, lchild.pairs, parent.pairs, rchild.pairs, from_left?)

      lchild = %{lchild | pairs: lpairs}
      rchild = %{rchild | pairs: rpairs}
      {:ok, lchild_loc} = Log.put_node(log, lchild)
      {:ok, rchild_loc} = Log.put_node(log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_loc)
        |> List.replace_at(idx + 1, rchild_loc)

      parent = %{parent | pairs: pairs, children: children}
      {:normal, parent}
    else
      # sibling is minimal, merge keys 
      {merged, pairs} = merge_keys(idx, lchild.pairs, parent.pairs, rchild.pairs)
      merged_child = %BTree{is_leaf: true, pairs: merged, capacity: parent.capacity}
      {:ok, merge_child_loc} = Log.put_node(log, merged_child)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, merge_child_loc)

      parent = %{parent | pairs: pairs, children: children}

      if is_minimal(parent.capacity, parent.pairs) do
        {:underflow, parent, pair}
      else
        {:normal, parent}
      end
    end
  end

  defp handle_underflow(log, pair, idx, lchild, parent, rchild, from_left?) do
    is_minimal =
      if from_left?,
        do: is_minimal(rchild.capacity, rchild.pairs),
        else: is_minimal(lchild.capacity, lchild.pairs)

    if not is_minimal do
      # sibling not minimal, rotate keys
      {lpairs, pairs, rpairs} =
        rotate_keys(idx, lchild.pairs, parent.pairs, rchild.pairs, from_left?)

      {lchildren, rchildren} = rotate_children(lchild.children, rchild.children, from_left?)

      lchild = %{lchild | pairs: lpairs, children: lchildren}
      rchild = %{rchild | pairs: rpairs, children: rchildren}

      {:ok, lchild_loc} = Log.put_node(log, lchild)
      {:ok, rchild_loc} = Log.put_node(log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_loc)
        |> List.replace_at(idx + 1, rchild_loc)

      parent = %{parent | pairs: pairs, children: children}
      {:normal, parent}
    else
      # sibling is minimal, merge nodes
      {merged, pairs, merged_children} =
        merge_nodes(
          idx,
          lchild.pairs,
          parent.pairs,
          rchild.pairs,
          lchild.children,
          rchild.children
        )

      merged_child =
        %BTree{
          is_leaf: false,
          capacity: parent.capacity,
          pairs: merged,
          children: merged_children
        }

      {:ok, merged_child_loc} = Log.put_node(log, merged_child)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, merged_child_loc)

      parent = %{parent | pairs: pairs, children: children}

      if is_minimal(parent.capacity, parent.pairs) do
        {:underflow, parent, pair}
      else
        {:normal, parent}
      end
    end
  end

  defp rotate_keys(idx, lpairs, pairs, rpairs, from_left?) do
    if from_left? do
      {up_pair, rpairs} = List.pop_at(rpairs, 0)
      down_pair = Enum.at(pairs, idx)
      pairs = List.replace_at(pairs, idx, up_pair)
      lpairs = List.insert_at(lpairs, -1, down_pair)
      {lpairs, pairs, rpairs}
    else
      {up_pair, lpairs} = List.pop_at(lpairs, -1)
      down_pair = Enum.at(pairs, idx)
      pairs = List.replace_at(pairs, idx, up_pair)
      rpairs = List.insert_at(rpairs, 0, down_pair)
      {lpairs, pairs, rpairs}
    end
  end

  defp rotate_children(lchildren, rchildren, from_left?) do
    if from_left? do
      # take first child from right, append to left
      [first_child | rchildren] = rchildren
      lchildren = lchildren ++ [first_child]
      {lchildren, rchildren}
    else
      # take last child from left, prepend to right
      {last_child, lchildren} = List.pop_at(lchildren, -1)
      rchildren = [last_child | rchildren]
      {lchildren, rchildren}
    end
  end

  defp merge_keys(idx, lpairs, pairs, rpairs) do
    {down_pair, pairs} = List.pop_at(pairs, idx)
    merged = lpairs ++ [down_pair] ++ rpairs
    {merged, pairs}
  end

  defp merge_nodes(idx, lpairs, pairs, rpairs, lchildren, rchildren) do
    {merged_pairs, pairs} = merge_keys(idx, lpairs, pairs, rpairs)
    {merged_pairs, pairs, lchildren ++ rchildren}
  end

  defp is_minimal(cap, pairs) do
    length(pairs) < div(cap, 2)
  end

  defp remove_last(_log, %{is_leaf: true} = node) do
    {pair, pairs} = List.pop_at(node.pairs, -1)
    node = %{node | pairs: pairs}

    if is_minimal(node.capacity, node.pairs) do
      {:underflow, node, pair}
    else
      {:normal, node, pair}
    end
  end

  defp remove_last(log, node) do
    last_child_loc = Enum.at(node.children, -1)
    {:ok, child} = Log.get_node(log, last_child_loc)

    case remove_last(log, child) do
      {:normal, pair, child} ->
        {:ok, child_loc} = Log.put_node(log, child)
        children = List.replace_at(node.children, -1, child_loc)
        node = %{node | children: children}
        {:normal, node, pair}

      {:underflow, child, pair} ->
        idx = length(node.pairs) - 1
        lchild_loc = Enum.at(node.children, -2)
        {:ok, lchild} = Log.get_node(log, lchild_loc)

        case handle_underflow(log, pair, idx, lchild, node, child, false) do
          {:normal, node} ->
            {:normal, node, pair}

          {:underflow, node, _pair} ->
            {:underflow, node, pair}
        end
    end
  end

  defp search_index([], _key, idx), do: {:next, idx}
  defp search_index([{k, _v} | _pairs], key, idx) when k == key, do: {:exact, idx}

  defp search_index([{k, _v} | pairs], key, idx) when k < key,
    do: search_index(pairs, key, idx + 1)

  defp search_index([{k, _v} | _pairs], key, idx) when k > key, do: {:next, idx}
end
