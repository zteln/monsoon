defmodule Monsoon.BTree.Remove do
  alias Monsoon.BTree.{Leaf, Interior, Util}
  alias Monsoon.Log

  def remove(%Leaf{} = node, key, extra) do
    case Util.search_index(node.keys, key, 0) do
      {:exact, idx} ->
        # key found, delete
        keys = List.delete_at(node.keys, idx)
        values = List.delete_at(node.values, idx)
        node = %{node | keys: keys, values: values}

        if has_underflow(node) do
          {:underflow, node, extra}
        else
          {:normal, node, extra}
        end

      _ ->
        # key not found
        nil
    end
  end

  def remove(%Interior{} = node, key, extra) do
    idx = Util.find_child_index(node.keys, key)
    child_loc = Enum.at(node.children, idx)
    {:ok, child} = Log.get_node(extra.log, child_loc)

    case remove(child, key, extra) do
      nil ->
        nil

      {:normal, child, extra} ->
        {:ok, child_loc} = Log.put_node(extra.log, child)
        children = List.replace_at(node.children, idx, child_loc)
        node = %{node | children: children}
        {:normal, node, extra}

      {:underflow, child, extra} ->
        if idx < length(node.keys) do
          # has right sibling
          rchild_loc = Enum.at(node.children, idx + 1)
          {:ok, rchild} = Log.get_node(extra.log, rchild_loc)
          handle_underflow(idx, child, node, rchild, true, extra)
        else
          # child is last, take left sibling
          idx = idx - 1
          lchild_loc = Enum.at(node.children, idx)
          {:ok, lchild} = Log.get_node(extra.log, lchild_loc)
          handle_underflow(idx, lchild, node, child, false, extra)
        end
    end
  end

  defp handle_underflow(idx, %Leaf{} = lchild, parent, %Leaf{} = rchild, from_left?, extra) do
    has_underflow = if from_left?, do: has_underflow(rchild), else: has_underflow(lchild)

    if not has_underflow do
      # sibling is not minimal, rotate keys
      {lchild, parent, rchild} = rotate_leafs(idx, lchild, parent, rchild, from_left?)

      {:ok, lchild_loc} = Log.put_node(extra.log, lchild)
      {:ok, rchild_loc} = Log.put_node(extra.log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_loc)
        |> List.replace_at(idx + 1, rchild_loc)

      parent = %{parent | children: children}
      {:normal, parent, extra}
    else
      # sibling is minimal, merge keys
      {mchild, parent} = merge_leafs(idx, lchild, parent, rchild)
      extra = merge_leaf_links(lchild, mchild, rchild, extra)
      {:ok, mchild_loc} = Log.put_node(extra.log, mchild)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, mchild_loc)

      parent = %{parent | children: children}

      if has_underflow(parent) do
        {:underflow, parent, extra}
      else
        {:normal, parent, extra}
      end
    end
  end

  defp handle_underflow(idx, lchild, parent, rchild, from_left?, extra) do
    has_underflow =
      if from_left?,
        do: has_underflow(rchild),
        else: has_underflow(lchild)

    if not has_underflow do
      # rotate interior nodes
      {lchild, parent, rchild} = rotate_interior(idx, lchild, parent, rchild, from_left?)

      {:ok, lchild_loc} = Log.put_node(extra.log, lchild)
      {:ok, rchild_loc} = Log.put_node(extra.log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_loc)
        |> List.replace_at(idx + 1, rchild_loc)

      parent = %{parent | children: children}
      {:normal, parent, extra}
    else
      # merge interior nodes
      {mchild, parent} = merge_interior(idx, lchild, parent, rchild)

      {:ok, mchild_loc} = Log.put_node(extra.log, mchild)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, mchild_loc)

      parent = %{parent | children: children}

      if has_underflow(parent) do
        {:underflow, parent, extra}
      else
        {:normal, parent, extra}
      end
    end
  end

  defp merge_interior(idx, lchild, parent, rchild) do
    {down_key, keys} = List.pop_at(parent.keys, idx)
    merged_keys = lchild.keys ++ [down_key] ++ rchild.keys
    merged_children = lchild.children ++ rchild.children

    mchild = %Interior{
      keys: merged_keys,
      children: merged_children,
      capacity: parent.capacity
    }

    parent = %{parent | keys: keys}
    {mchild, parent}
  end

  defp rotate_interior(idx, lchild, parent, rchild, from_left?) do
    if from_left? do
      [up_key | rkeys] = rchild.keys
      [left_child | rchildren] = rchild.children
      down_key = Enum.at(parent.keys, idx)
      keys = List.replace_at(parent.keys, idx, up_key)
      lkeys = lchild.keys ++ [down_key]
      lchildren = lchild.children ++ [left_child]
      lchild = %{lchild | keys: lkeys, children: lchildren}
      parent = %{parent | keys: keys}
      rchild = %{rchild | keys: rkeys, children: rchildren}
      {lchild, parent, rchild}
    else
      {up_key, lkeys} = List.pop_at(lchild.keys, -1)
      {right_child, lchildren} = List.pop_at(lchild.children, -1)
      down_key = Enum.at(parent.keys, idx)
      keys = List.replace_at(parent.keys, idx, up_key)
      rkeys = [down_key | rchild.keys]
      rchildren = [right_child | rchild.children]
      lchild = %{lchild | keys: lkeys, children: lchildren}
      parent = %{parent | keys: keys}
      rchild = %{rchild | keys: rkeys, children: rchildren}
      {lchild, parent, rchild}
    end
  end

  defp merge_leafs(idx, lchild, parent, rchild) do
    keys = List.delete_at(parent.keys, idx)
    merged_keys = lchild.keys ++ rchild.keys
    merged_values = lchild.values ++ rchild.values

    mchild = %{
      Leaf.new()
      | keys: merged_keys,
        values: merged_values,
        capacity: parent.capacity
    }

    parent = %{parent | keys: keys}
    {mchild, parent}
  end

  defp rotate_leafs(idx, lchild, parent, rchild, from_left?) do
    if from_left? do
      [left_key | rkeys] = rchild.keys
      [left_value | rvalues] = rchild.values
      keys = List.replace_at(parent.keys, idx, hd(rkeys))
      lkeys = lchild.keys ++ [left_key]
      lvalues = lchild.values ++ [left_value]
      lchild = %{lchild | keys: lkeys, values: lvalues}
      parent = %{parent | keys: keys}
      rchild = %{rchild | keys: rkeys, values: rvalues}
      {lchild, parent, rchild}
    else
      {right_key, lkeys} = List.pop_at(lchild.keys, -1)
      {right_value, lvalues} = List.pop_at(lchild.values, -1)
      keys = List.replace_at(parent.keys, idx, right_key)
      rkeys = [right_key | rchild.keys]
      rvalues = [right_value | rchild.values]
      lchild = %{lchild | keys: lkeys, values: lvalues}
      parent = %{parent | keys: keys}
      rchild = %{rchild | keys: rkeys, values: rvalues}
      {lchild, parent, rchild}
    end
  end

  defp merge_leaf_links(lchild, mchild, rchild, extra) do
    %{btree: {root_loc, leaf_links_loc, metadata_loc}} = extra
    {:ok, leaf_links} = Log.get_leaf_links(extra.log, leaf_links_loc)

    {prev, _next} = Map.get(leaf_links, lchild.id)
    {_prev, next} = Map.get(leaf_links, rchild.id)

    leaf_links =
      case {prev, next} do
        {nil, nil} ->
          leaf_links

        {prev, nil} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)

          leaf_links
          |> Map.put(prev, {prev_prev, mchild.id})

        {nil, next} ->
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(next, {mchild.id, next_next})

        {prev, next} ->
          {prev_prev, _prev_next} = Map.get(leaf_links, prev)
          {_next_prev, next_next} = Map.get(leaf_links, next)

          leaf_links
          |> Map.put(prev, {prev_prev, mchild.id})
          |> Map.put(next, {mchild.id, next_next})
      end
      |> Map.delete(lchild.id)
      |> Map.delete(rchild.id)
      |> Map.put(mchild.id, {prev, next})

    {:ok, leaf_links_loc} = Log.put_leaf_links(extra.log, leaf_links)

    %{extra | btree: {root_loc, leaf_links_loc, metadata_loc}}
  end

  defp has_underflow(node), do: length(node.keys) < div(node.capacity, 2)
end
