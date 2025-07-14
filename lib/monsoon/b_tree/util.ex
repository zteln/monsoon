defmodule Monsoon.BTree.Util do
  def search_index([], _key, idx), do: {:next, idx}
  def search_index([k | _keys], key, idx) when k == key, do: {:exact, idx}

  def search_index([k | keys], key, idx) when k < key,
    do: search_index(keys, key, idx + 1)

  def search_index([k | _keys], key, idx) when k > key, do: {:next, idx}

  def find_child_index(keys, key) do
    case search_index(keys, key, 0) do
      {:exact, idx} -> idx + 1
      {:next, idx} -> idx
    end
  end
end
