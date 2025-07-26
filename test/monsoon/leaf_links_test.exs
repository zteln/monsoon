defmodule Monsoon.LeafLinksTest do
  use ExUnit.Case, async: true
  alias Monsoon.LeafLinks
  alias Monsoon.Log
  alias Monsoon.BTree.Leaf

  @moduletag :tmp_dir

  setup c do
    file_path = Path.join(c.tmp_dir, "log")
    {:ok, log} = Log.new(file_path)
    %{log: log, log_path: file_path}
  end

  describe "split/5" do
    test "splits single leaf link", c do
      node = %Leaf{id: 1}
      lnode = %Leaf{id: 2}
      rnode = %Leaf{id: 3}

      leaf_links = %{1 => {nil, nil}}
      leaf_links_bp = Log.put_leaf_links(c.log, leaf_links)
      Log.flush(c.log)

      assert {:ok, %{1 => {nil, nil}}} == Log.get_leaf_links(c.log, leaf_links_bp)

      assert {_, _} = leaf_links_bp = LeafLinks.split(leaf_links_bp, lnode, node, rnode, c.log)

      Log.flush(c.log)

      assert {:ok, %{2 => {nil, 3}, 3 => {2, nil}}} == Log.get_leaf_links(c.log, leaf_links_bp)
    end
  end

  describe "merge/5" do
    test "merges two leaf links", c do
      lnode = %Leaf{id: 1}
      rnode = %Leaf{id: 2}
      mnode = %Leaf{id: 3}

      leaf_links = %{1 => {nil, 2}, 2 => {1, nil}}
      leaf_links_bp = Log.put_leaf_links(c.log, leaf_links)
      Log.flush(c.log)

      assert {_, _} = leaf_links_bp = LeafLinks.merge(leaf_links_bp, lnode, mnode, rnode, c.log)

      Log.flush(c.log)

      assert {:ok, %{3 => {nil, nil}}} == Log.get_leaf_links(c.log, leaf_links_bp)
    end
  end
end
