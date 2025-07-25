defmodule Monsoon.BTree.Interior do
  alias Monsoon.BTree.Range

  defstruct [
    :capacity,
    :keys,
    children: []
  ]

  @type t :: %__MODULE__{
          capacity: non_neg_integer(),
          keys: [term()],
          children: [Monsoon.Log.block_pointer()]
        }

  def new do
    %__MODULE__{keys: %Range{}}
  end

  def has_overflow(interior) do
    Range.has_overflow(interior.keys, interior.capacity)
  end

  def has_underflow(interior) do
    Range.has_underflow(interior.keys, interior.capacity)
  end

  def insert_key_at(interior, idx, key) do
    keys = Range.add(interior.keys, idx, key)
    %{interior | keys: keys}
  end

  def replace_key_at(interior, idx, key) do
    keys = Range.replace(interior.keys, idx, key)
    %{interior | keys: keys}
  end

  def delete_key_at(interior, idx) do
    keys = Range.delete(interior.keys, idx)
    %{interior | keys: keys}
  end

  def search(interior, key) do
    Range.find_index(interior.keys, key)
  end

  def find_child_index(interior, key) do
    case Range.find_index(interior.keys, key) do
      {:exact, idx} -> idx + 1
      {:next, idx} -> idx
    end
  end

  def split(interior, lchild_bp, rchild_bp, idx, sep_key) do
    k = div(interior.capacity, 2)

    children =
      interior.children
      |> List.replace_at(idx, lchild_bp)
      |> List.insert_at(idx + 1, rchild_bp)

    {lchildren, rchildren} = Enum.split(children, k + 1)

    {lkeys, split_key, rkeys} =
      interior.keys
      |> Range.add(idx, sep_key)
      |> Range.split(k)

    left = %__MODULE__{
      capacity: interior.capacity,
      keys: lkeys,
      children: lchildren
    }

    right = %__MODULE__{
      capacity: interior.capacity,
      keys: rkeys,
      children: rchildren
    }

    {left, split_key, right}
  end

  def rotate(parent, lchild, rchild, idx, from_left?) do
    {lkeys, lchildren, keys, rkeys, rchildren} =
      if from_left? do
        {rkeys, up_key} = Range.pop_at(rchild.keys, 0)
        down_key = Range.peek_at(parent.keys, idx)
        keys = Range.replace(parent.keys, idx, up_key)
        lkeys = Range.add(lchild.keys, -1, down_key)

        [to_left | rchildren] = rchild.children
        lchildren = List.insert_at(lchild.children, -1, to_left)

        {lkeys, lchildren, keys, rkeys, rchildren}
      else
        {lkeys, up_key} = Range.pop_at(lchild.keys, -1)
        down_key = Range.peek_at(parent.keys, idx)
        keys = Range.replace(parent.keys, idx, up_key)
        rkeys = Range.add(rchild.keys, 0, down_key)

        {to_right, lchildren} = List.pop_at(lchild.children, -1)
        rchildren = [to_right | rchild.children]

        {lkeys, lchildren, keys, rkeys, rchildren}
      end

    lchild = %{lchild | keys: lkeys, children: lchildren}
    parent = %{parent | keys: keys}
    rchild = %{rchild | keys: rkeys, children: rchildren}
    {lchild, parent, rchild}
  end

  def merge(parent, lchild, rchild, idx) do
    {keys, down_key} = Range.pop_at(parent.keys, idx)
    merged_keys = Range.merge(lchild.keys, rchild.keys, down_key)
    merged_children = lchild.children ++ rchild.children

    merged_child = %__MODULE__{
      keys: merged_keys,
      children: merged_children,
      capacity: parent.capacity
    }

    parent = %{parent | keys: keys}
    {merged_child, parent}
  end
end
