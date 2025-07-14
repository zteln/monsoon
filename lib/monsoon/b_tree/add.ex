defmodule Monsoon.BTree.Add do
  alias Monsoon.BTree.{Leaf, Interior, Util}
  alias Monsoon.Log

  # @spec add(log :: Log.t(), node :: Leaf.t() | Interior.t(), key :: term(), value :: term()) ::
  #         {:normal, loc :: BTree.location()}
  #         | {:split, {left_loc :: BTree.location(), key :: term(), right_loc :: BTree.location()}}
  def add(%Leaf{} = node, key, value, extra) do
    case Util.search_index(node.keys, key, 0) do
      {:exact, idx} ->
        # update existing key
        keys = List.replace_at(node.keys, idx, key)
        values = List.replace_at(node.values, idx, value)
        node = %{node | keys: keys, values: values}
        {:normal, node, extra}

      {:next, idx} ->
        # insert new key
        if is_full(node) do
          handle_leaf_split(node, idx, key, value, extra)
        else
          keys = List.insert_at(node.keys, idx, key)
          values = List.insert_at(node.values, idx, value)
          node = %{node | keys: keys, values: values}
          {:normal, node, extra}
        end
    end
  end

  def add(%Interior{} = node, key, value, extra) do
    # always add in leaf
    {idx, cidx} =
      case Util.search_index(node.keys, key, 0) do
        {:exact, idx} -> {idx, idx + 1}
        {:next, idx} -> {idx, idx}
      end

    child_loc = Enum.at(node.children, cidx)
    {:ok, child} = Log.get_node(extra.log, child_loc)

    case add(child, key, value, extra) do
      {:normal, child, extra} ->
        {:ok, child_loc} = Log.put_node(extra.log, child)
        children = List.replace_at(node.children, cidx, child_loc)
        node = %{node | children: children}
        {:normal, node, extra}

      {:split, {lnode, split_key, rnode}, extra} ->
        if is_full(node) do
          # full interior node, split node
          {:ok, lnode_loc} = Log.put_node(extra.log, lnode)
          {:ok, rnode_loc} = Log.put_node(extra.log, rnode)
          handle_interior_split(node, cidx, split_key, lnode_loc, rnode_loc, extra)
        else
          # not full, insert key
          keys = List.insert_at(node.keys, idx, split_key)

          {:ok, lnode_loc} = Log.put_node(extra.log, lnode)
          {:ok, rnode_loc} = Log.put_node(extra.log, rnode)

          children =
            node.children
            |> List.replace_at(idx, lnode_loc)
            |> List.insert_at(idx + 1, rnode_loc)

          node = %{node | keys: keys, children: children}
          {:normal, node, extra}
        end
    end
  end

  defp handle_interior_split(node, idx, key, lnode_loc, rnode_loc, extra) do
    {left, split_key, right} = split_interior(node, idx, key, lnode_loc, rnode_loc)
    {:split, {left, split_key, right}, extra}
  end

  defp split_interior(node, idx, sep_key, lnode_loc, rnode_loc) do
    all_keys = List.insert_at(node.keys, idx, sep_key)

    all_children =
      node.children
      |> List.replace_at(idx, lnode_loc)
      |> List.insert_at(idx + 1, rnode_loc)

    k = div(length(all_keys), 2)

    {lkeys, [split_key | rkeys]} = Enum.split(all_keys, k)

    {lchildren, rchildren} = Enum.split(all_children, k + 1)

    left = %Interior{
      capacity: node.capacity,
      keys: lkeys,
      children: lchildren
    }

    right = %Interior{
      capacity: node.capacity,
      keys: rkeys,
      children: rchildren
    }

    {left, split_key, right}
  end

  defp handle_leaf_split(node, idx, key, value, extra) do
    {lnode, split_key, rnode} = split_leaf(node, idx, key, value)
    extra = split_leaf_links(lnode, node, rnode, extra)

    {:split, {lnode, split_key, rnode}, extra}
  end

  defp split_leaf(node, idx, key, value) do
    # full, split node
    k = div(node.capacity, 2)
    keys = List.insert_at(node.keys, idx, key)
    values = List.insert_at(node.values, idx, value)

    {lkeys, [split_key | _] = rkeys} = Enum.split(keys, k)
    {lvalues, rvalues} = Enum.split(values, k)

    lnode = %{
      Leaf.new()
      | capacity: node.capacity,
        keys: lkeys,
        values: lvalues
    }

    rnode = %{
      Leaf.new()
      | capacity: node.capacity,
        keys: rkeys,
        values: rvalues
    }

    {lnode, split_key, rnode}
  end

  defp split_leaf_links(lnode, node, rnode, extra) do
    %{btree: {root_loc, leaf_links_loc, metadata_loc}} = extra
    {:ok, leaf_links} = Log.get_leaf_links(extra.log, leaf_links_loc)

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

    {:ok, leaf_links_loc} = Log.put_leaf_links(extra.log, leaf_links)

    %{extra | btree: {root_loc, leaf_links_loc, metadata_loc}}
  end

  defp is_full(node), do: length(node.keys) >= node.capacity - 1
end
