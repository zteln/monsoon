defmodule Monsoon.BTree do
  @moduledoc """

  """
  alias Monsoon.Log
  alias __MODULE__.{Leaf, Interior, Range}

  @db_file_name "db.monsoon"
  @db_tmp_file_name "tmp.monsoon"

  defstruct [
    :root_bp,
    :leaf_links_bp,
    :metadata_bp,
    :log
  ]

  @type t :: %__MODULE__{
          root_bp: Log.block_pointer(),
          leaf_links_bp: Log.block_pointer(),
          metadata_bp: Log.block_pointer(),
          log: Log.t()
        }
  @type btree_node :: Leaf.t() | Interior.t()
  @type bps :: {Log.block_pointer(), Log.block_pointer(), Log.block_pointer()}

  @spec new(dir :: String.t(), capacity :: non_neg_integer()) ::
          {:ok, t()} | {:error, term()}
  def new(dir, capacity) do
    with {:ok, log} <- new_log(Path.join(dir, @db_file_name)),
         {:ok, root_bp, leaf_links_bp, metadata_bp} <- init_log(log, capacity) do
      {:ok,
       %__MODULE__{
         root_bp: root_bp,
         leaf_links_bp: leaf_links_bp,
         metadata_bp: metadata_bp,
         log: log
       }}
    end
  end

  defp new_log(file_path) do
    Log.new(file_path)
  end

  defp init_log(log, capacity) do
    case Log.get_commit(log) do
      {:ok, _root_bp, _leaf_links_bp, _metadata_bp} = res ->
        res

      {:error, :eof} ->
        root = %{Leaf.new() | capacity: capacity}
        leaf_links = %{root.id => {nil, nil}}
        metadata = []
        root_bp = Log.put_node(log, root)
        leaf_links_bp = Log.put_leaf_links(log, leaf_links)
        metadata_bp = Log.put_metadata(log, metadata)
        :ok = Log.flush(log)
        :ok = Log.commit(log, root_bp, leaf_links_bp, metadata_bp)
        {:ok, root_bp, leaf_links_bp, metadata_bp}

      {:error, _reason} = e ->
        e
    end
  end

  @spec commit(btree :: t()) :: :ok
  def commit(btree) do
    Log.commit(btree.log, btree.root_bp, btree.leaf_links_bp, btree.metadata_bp)
  end

  @spec copy(btree :: t(), dir :: String.t()) :: t()
  def copy(btree, dir) do
    {:ok, new_log} = new_log(Path.join(dir, @db_tmp_file_name))
    btree = copy_btree(btree, btree.log, new_log)
    new_log = Log.move(btree.log, new_log)
    %{btree | log: new_log}
  end

  defp copy_btree(btree, from, to) do
    root_bp = copy_from_root(from, to, btree.root_bp)
    leaf_links_bp = copy_leaf_links(from, to, btree.leaf_links_bp)
    metadata_bp = copy_metadata(from, to, btree.metadata_bp)
    :ok = Log.flush(to)
    :ok = Log.commit(to, root_bp, leaf_links_bp, metadata_bp)
    %{btree | root_bp: root_bp, leaf_links_bp: leaf_links_bp, metadata_bp: metadata_bp}
  end

  defp copy_metadata(from, to, metadata_bp) do
    {:ok, metadata} = Log.get_metadata(from, metadata_bp)
    Log.put_metadata(to, metadata)
  end

  defp copy_leaf_links(from, to, leaf_links_bp) do
    {:ok, leaf_links} = Log.get_leaf_links(from, leaf_links_bp)
    Log.put_leaf_links(to, leaf_links)
  end

  defp copy_from_root(from, to, root_bp) do
    {:ok, root} = Log.get_node(from, root_bp)
    update_children(from, to, root)
  end

  defp update_children(_from, to, %Leaf{} = node) do
    Log.put_node(to, node)
  end

  defp update_children(from, to, %Interior{} = node) do
    new_children =
      node.children
      |> Enum.map(fn child_bp ->
        {:ok, child} = Log.get_node(from, child_bp)
        update_children(from, to, child)
      end)

    node = %{node | children: new_children}
    Log.put_node(to, node)
  end

  @spec search(btree :: t(), key :: term()) :: term()
  def search(btree, key) do
    {:ok, root} = Log.get_node(btree.log, btree.root_bp)
    search_key(root, key, btree.log)
  end

  defp search_key(%Leaf{} = node, key, _log) do
    Leaf.get(node, key)
  end

  defp search_key(%Interior{} = node, key, log) do
    child_idx = Interior.find_child_index(node, key)
    child_bp = Enum.at(node.children, child_idx)
    {:ok, child} = Log.get_node(log, child_bp)
    search_key(child, key, log)
  end

  @spec select(info_f :: fun(), lower :: term(), upper :: term()) :: Stream.t()
  def select(info_f, lower, upper) do
    select_from_tree(info_f, lower, upper)
  end

  defp select_from_tree(info_f, lower, upper) do
    Stream.resource(
      fn ->
        btree = info_f.()

        {:ok, leaf_links} = Log.get_leaf_links(btree.log, btree.leaf_links_bp)

        {first, _} =
          Enum.find(leaf_links, fn
            {_id, {nil, _next}} -> true
            _ -> false
          end)

        to_list(first, lower, upper, leaf_links, btree.log)
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

    case Leaf.take_in_pairs(leaf, lower, upper) do
      {:cont, res} ->
        res ++ to_list(next, lower, upper, leaf_links, log)

      {:halt, res} ->
        res
    end
  end

  @spec add(btree :: t(), k :: term(), v :: term()) :: t()
  def add(btree, k, v) do
    {:ok, root} = Log.get_node(btree.log, btree.root_bp)

    case add_kv(btree, root, k, v) do
      {:normal, root, btree} ->
        root_bp = Log.put_node(btree.log, root)
        :ok = Log.flush(btree.log)
        %{btree | root_bp: root_bp}

      {:split, {lchild, split_k, rchild}, btree} ->
        lchild_bp = Log.put_node(btree.log, lchild)
        rchild_bp = Log.put_node(btree.log, rchild)
        keys = %Range{} |> Range.add(0, split_k)

        root = %Interior{
          capacity: root.capacity,
          keys: keys,
          children: [lchild_bp, rchild_bp]
        }

        root_bp = Log.put_node(btree.log, root)
        :ok = Log.flush(btree.log)
        %{btree | root_bp: root_bp}
    end
  end

  defp add_kv(btree, %Leaf{} = node, key, value) do
    case Leaf.search(node, key) do
      {:exact, idx} ->
        # update existing key
        node = Leaf.replace_pair_at(node, idx, {key, value})
        {:normal, node, btree}

      {:next, idx} ->
        # insert new key
        if Leaf.has_overflow(node) do
          handle_leaf_split(btree, node, idx, key, value)
        else
          node = Leaf.insert_pair_at(node, idx, {key, value})
          {:normal, node, btree}
        end
    end
  end

  defp add_kv(btree, %Interior{} = node, key, value) do
    # always add in leaf
    {idx, cidx} =
      case Interior.search(node, key) do
        {:exact, idx} -> {idx, idx + 1}
        {:next, idx} -> {idx, idx}
      end

    child_bp = Enum.at(node.children, cidx)
    {:ok, child} = Log.get_node(btree.log, child_bp)

    case add_kv(btree, child, key, value) do
      {:normal, child, btree} ->
        child_bp = Log.put_node(btree.log, child)
        children = List.replace_at(node.children, cidx, child_bp)
        node = %{node | children: children}
        {:normal, node, btree}

      {:split, {lnode, split_key, rnode}, btree} ->
        if Interior.has_overflow(node) do
          # full interior node, split node
          lnode_bp = Log.put_node(btree.log, lnode)
          rnode_bp = Log.put_node(btree.log, rnode)
          handle_interior_split(btree, node, cidx, split_key, lnode_bp, rnode_bp)
        else
          # not full, insert key
          node = Interior.insert_key_at(node, idx, split_key)

          lnode_bp = Log.put_node(btree.log, lnode)
          rnode_bp = Log.put_node(btree.log, rnode)

          children =
            node.children
            |> List.replace_at(idx, lnode_bp)
            |> List.insert_at(idx + 1, rnode_bp)

          node = %{node | children: children}
          {:normal, node, btree}
        end
    end
  end

  defp handle_interior_split(btree, node, idx, key, lnode_bp, rnode_bp) do
    {left, split_key, right} = Interior.split(node, lnode_bp, rnode_bp, idx, key)
    {:split, {left, split_key, right}, btree}
  end

  defp handle_leaf_split(btree, node, idx, key, value) do
    {lnode, split_key, rnode} = Leaf.split(node, idx, {key, value})
    btree = split_leaf_links(btree, lnode, node, rnode)
    {:split, {lnode, split_key, rnode}, btree}
  end

  defp split_leaf_links(btree, lnode, node, rnode) do
    {:ok, leaf_links} = Log.get_leaf_links(btree.log, btree.leaf_links_bp)

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

    leaf_links_bp = Log.put_leaf_links(btree.log, leaf_links)
    %{btree | leaf_links_bp: leaf_links_bp}
  end

  @spec remove(btree :: t(), key :: term()) :: t()
  def remove(btree, key) do
    {:ok, root} = Log.get_node(btree.log, btree.root_bp)

    case remove_k(btree, root, key) do
      nil ->
        btree

      {:normal, root, btree} ->
        root_bp = Log.put_node(btree.log, root)
        :ok = Log.flush(btree.log)
        %{btree | root_bp: root_bp}

      {:underflow, %Interior{keys: %Range{size: 0}, children: [root_bp]}, btree} ->
        :ok = Log.flush(btree.log)
        %{btree | root_bp: root_bp}

      {:underflow, root, btree} ->
        root_bp = Log.put_node(btree.log, root)
        :ok = Log.flush(btree.log)
        %{btree | root_bp: root_bp}
    end
  end

  defp remove_k(btree, %Leaf{} = node, key) do
    case Leaf.search(node, key) do
      {:exact, idx} ->
        # key found, delete
        node = Leaf.delete_pair_at(node, idx)

        if Leaf.has_underflow(node) do
          {:underflow, node, btree}
        else
          {:normal, node, btree}
        end

      _ ->
        # key not found
        nil
    end
  end

  defp remove_k(btree, %Interior{} = node, key) do
    idx = Interior.find_child_index(node, key)
    child_bp = Enum.at(node.children, idx)
    {:ok, child} = Log.get_node(btree.log, child_bp)

    case remove_k(btree, child, key) do
      nil ->
        nil

      {:normal, child, btree} ->
        child_bp = Log.put_node(btree.log, child)
        children = List.replace_at(node.children, idx, child_bp)
        node = %{node | children: children}
        {:normal, node, btree}

      {:underflow, child, extra} ->
        if idx < node.keys.size do
          # has right sibling
          rchild_bp = Enum.at(node.children, idx + 1)
          {:ok, rchild} = Log.get_node(extra.log, rchild_bp)
          handle_underflow(btree, idx, child, node, rchild, true)
        else
          # child is last, take left sibling
          idx = idx - 1
          lchild_bp = Enum.at(node.children, idx)
          {:ok, lchild} = Log.get_node(extra.log, lchild_bp)
          handle_underflow(btree, idx, lchild, node, child, false)
        end
    end
  end

  defp handle_underflow(btree, idx, %Leaf{} = lchild, parent, %Leaf{} = rchild, from_left?) do
    has_underflow =
      if from_left?, do: Leaf.has_underflow(rchild), else: Leaf.has_underflow(lchild)

    if not has_underflow do
      # sibling is not minimal, rotate keys
      {lchild, parent, rchild} = Leaf.rotate(lchild, rchild, parent, idx, from_left?)

      lchild_bp = Log.put_node(btree.log, lchild)
      rchild_bp = Log.put_node(btree.log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_bp)
        |> List.replace_at(idx + 1, rchild_bp)

      parent = %{parent | children: children}
      {:normal, parent, btree}
    else
      # sibling is minimal, merge keys
      {mchild, parent} = Leaf.merge(lchild, rchild, parent, idx)
      btree = merge_leaf_links(btree, lchild, mchild, rchild)
      mchild_bp = Log.put_node(btree.log, mchild)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, mchild_bp)

      parent = %{parent | children: children}

      if Interior.has_underflow(parent) do
        {:underflow, parent, btree}
      else
        {:normal, parent, btree}
      end
    end
  end

  defp handle_underflow(btree, idx, lchild, parent, rchild, from_left?) do
    has_underflow =
      if from_left?,
        do: Interior.has_underflow(rchild),
        else: Interior.has_underflow(lchild)

    if not has_underflow do
      # rotate interior nodes
      {lchild, parent, rchild} = Interior.rotate(parent, lchild, rchild, idx, from_left?)

      lchild_bp = Log.put_node(btree.log, lchild)
      rchild_bp = Log.put_node(btree.log, rchild)

      children =
        parent.children
        |> List.replace_at(idx, lchild_bp)
        |> List.replace_at(idx + 1, rchild_bp)

      parent = %{parent | children: children}
      {:normal, parent, btree}
    else
      # merge interior nodes

      {mchild, parent} = Interior.merge(parent, lchild, rchild, idx)

      mchild_bp = Log.put_node(btree.log, mchild)

      children =
        parent.children
        |> List.delete_at(idx + 1)
        |> List.replace_at(idx, mchild_bp)

      parent = %{parent | children: children}

      if Interior.has_underflow(parent) do
        {:underflow, parent, btree}
      else
        {:normal, parent, btree}
      end
    end
  end

  defp merge_leaf_links(btree, lchild, mchild, rchild) do
    {:ok, leaf_links} = Log.get_leaf_links(btree.log, btree.leaf_links_bp)

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

    leaf_links_bp = Log.put_leaf_links(btree.log, leaf_links)
    %{btree | leaf_links_bp: leaf_links_bp}
  end

  @spec put_metadata(t(), keyword()) :: t()
  def put_metadata(btree, metadata) do
    metadata_bp = Log.put_metadata(btree.log, metadata)
    :ok = Log.flush(btree.log)
    %{btree | metadata_bp: metadata_bp}
  end

  @spec get_metadata(t()) :: {:ok, keyword()} | {:error, term()}
  def get_metadata(btree) do
    Log.get_metadata(btree.log, btree.metadata_bp)
  end
end
