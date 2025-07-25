defmodule Monsoon.BTree.Leaf do
  alias Monsoon.BTree.Range
  alias Monsoon.BTree.Interior

  defstruct [
    :id,
    :capacity,
    :pairs,
    keys: [],
    values: []
  ]

  @type t :: %__MODULE__{
          id: binary(),
          capacity: non_neg_integer(),
          keys: [term()],
          values: [term()]
        }

  def new do
    %__MODULE__{id: gen_id(), pairs: %Range{}}
  end

  def has_overflow(leaf) do
    Range.has_overflow(leaf.pairs, leaf.capacity)
  end

  def has_underflow(leaf) do
    Range.has_underflow(leaf.pairs, leaf.capacity)
  end

  def insert_pair_at(leaf, idx, pair) do
    pairs = Range.add(leaf.pairs, idx, pair)
    %{leaf | pairs: pairs}
  end

  def replace_pair_at(leaf, idx, pair) do
    pairs = Range.replace(leaf.pairs, idx, pair)
    %{leaf | pairs: pairs}
  end

  def delete_pair_at(leaf, idx) do
    pairs = Range.delete(leaf.pairs, idx)
    %{leaf | pairs: pairs}
  end

  def get(leaf, key) do
    case Range.find(leaf.pairs, key, &elem(&1, 0)) do
      nil ->
        nil

      {_key, value} ->
        value
    end
  end

  def search(leaf, key) do
    Range.find_index(leaf.pairs, key, &elem(&1, 0))
  end

  def split(leaf, idx, pair) do
    k = div(leaf.capacity, 2)
    pairs = Range.add(leaf.pairs, idx, pair)
    {lpairs, {split_key, _}, rpairs} = Range.split(pairs, k, true)

    lleaf = %{
      new()
      | capacity: leaf.capacity,
        pairs: lpairs
    }

    rleaf = %{
      new()
      | capacity: leaf.capacity,
        pairs: rpairs
    }

    {lleaf, split_key, rleaf}
  end

  def rotate(lleaf, rleaf, %Interior{} = parent, idx, from_left?) do
    {lpairs, keys, rpairs} =
      if from_left? do
        {rpairs, to_left} = Range.pop_at(rleaf.pairs, 0)
        {up_key, _} = Range.peek_at(rpairs, 0)
        keys = Range.replace(parent.keys, idx, up_key)
        lpairs = Range.add(lleaf.pairs, -1, to_left)
        {lpairs, keys, rpairs}
      else
        {lpairs, {key, _} = to_right} = Range.pop_at(lleaf.pairs, -1)
        keys = Range.replace(parent.keys, idx, key)
        rpairs = Range.add(rleaf.pairs, 0, to_right)
        {lpairs, keys, rpairs}
      end

    lleaf = %{lleaf | pairs: lpairs}
    parent = %{parent | keys: keys}
    rleaf = %{rleaf | pairs: rpairs}
    {lleaf, parent, rleaf}
  end

  def merge(lleaf, rleaf, %Interior{} = parent, idx) do
    keys = Range.delete(parent.keys, idx)
    merged_pairs = Range.merge(lleaf.pairs, rleaf.pairs)

    merged_leaf = %{
      new()
      | pairs: merged_pairs,
        capacity: parent.capacity
    }

    parent = %{parent | keys: keys}
    {merged_leaf, parent}
  end

  def take_in_pairs(leaf, lower, upper) do
    Range.take(leaf.pairs, lower, upper, &elem(&1, 0))
  end

  defp gen_id do
    :crypto.strong_rand_bytes(8)
  end
end
