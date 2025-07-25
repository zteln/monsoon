defmodule Monsoon.BtreeTest do
  use ExUnit.Case, async: true
  alias Monsoon.BTree
  alias Monsoon.Log

  @moduletag :tmp_dir

  setup c do
    {:ok, btree} = BTree.new(c.tmp_dir, 4)

    %{btree: btree}
  end

  describe "add/3" do
    test "adds in under full leaf node", c do
      assert %BTree{} = btree = BTree.add(c.btree, "k-1", "v-1")
      assert "v-1" = BTree.search(btree, "k-1")
    end

    test "splits full leaf node", c do
      btree =
        for n <- 1..5, reduce: c.btree do
          acc ->
            assert %BTree{} = btree = BTree.add(acc, n, "v-#{n}")
            btree
        end

      assert {:ok, %BTree.Interior{keys: %{list: [3]}, children: [left_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {2, "v-2"}]}}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{3, "v-3"}, {4, "v-4"}, {5, "v-5"}]}}} =
               Log.get_node(c.btree.log, right_bp)
    end

    test "adds to interior node", c do
      btree =
        for n <- 1..7, reduce: c.btree do
          acc ->
            assert %BTree{} = btree = BTree.add(acc, n, "v-#{n}")
            btree
        end

      assert {:ok,
              %BTree.Interior{keys: %{list: [3, 5]}, children: [left_bp, middle_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {2, "v-2"}]}}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{3, "v-3"}, {4, "v-4"}]}}} =
               Log.get_node(c.btree.log, middle_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{5, "v-5"}, {6, "v-6"}, {7, "v-7"}]}}} =
               Log.get_node(c.btree.log, right_bp)
    end

    test "splits interior node", c do
      btree =
        for n <- 1..11, reduce: c.btree do
          acc ->
            assert %BTree{} = btree = BTree.add(acc, n, "v-#{n}")
            btree
        end

      assert {:ok, %BTree.Interior{keys: %{list: [7]}, children: [left_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok,
              %BTree.Interior{keys: %{list: [3, 5]}, children: [l_left_bp, l_mid_bp, l_right_bp]}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {2, "v-2"}]}}} =
               Log.get_node(c.btree.log, l_left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{3, "v-3"}, {4, "v-4"}]}}} =
               Log.get_node(c.btree.log, l_mid_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{5, "v-5"}, {6, "v-6"}]}}} =
               Log.get_node(c.btree.log, l_right_bp)

      assert {:ok, %BTree.Interior{keys: %{list: [9]}, children: [r_left_bp, r_right_bp]}} =
               Log.get_node(c.btree.log, right_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{7, "v-7"}, {8, "v-8"}]}}} =
               Log.get_node(c.btree.log, r_left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{9, "v-9"}, {10, "v-10"}, {11, "v-11"}]}}} =
               Log.get_node(c.btree.log, r_right_bp)
    end
  end

  describe "remove/3" do
    test "removes key in leaf node", c do
      btree = BTree.add(c.btree, "k-1", "v-1")
      assert %BTree{} = btree = BTree.remove(btree, "k-1")
      assert {:ok, %BTree.Leaf{keys: [], values: []}} = Log.get_node(c.btree.log, btree.root_bp)
    end

    test "with underflow in one sibling causes key rotation", c do
      btree =
        for n <- 1..5, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      assert {:ok, %BTree.Interior{keys: %{list: [3]}, children: [left_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {2, "v-2"}]}}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{3, "v-3"}, {4, "v-4"}, {5, "v-5"}]}}} =
               Log.get_node(c.btree.log, right_bp)

      assert %BTree{} = btree = BTree.remove(btree, 2)

      assert {:ok, %BTree.Interior{keys: %{list: [4]}, children: [left_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {3, "v-3"}]}}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{4, "v-4"}, {5, "v-5"}]}}} =
               Log.get_node(c.btree.log, right_bp)
    end

    test "with underflow in both siblings causes key merge", c do
      btree =
        for n <- 1..4, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      assert {:ok, %BTree.Interior{keys: %{list: [3]}, children: [left_bp, right_bp]}} =
               Log.get_node(c.btree.log, btree.root_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {2, "v-2"}]}}} =
               Log.get_node(c.btree.log, left_bp)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{3, "v-3"}, {4, "v-4"}]}}} =
               Log.get_node(c.btree.log, right_bp)

      assert %BTree{} = btree = BTree.remove(btree, 2)
      assert %BTree{} = btree = BTree.remove(btree, 3)

      assert {:ok, %BTree.Leaf{pairs: %{list: [{1, "v-1"}, {4, "v-4"}]}}} =
               Log.get_node(c.btree.log, btree.root_bp)
    end
  end

  describe "search/3" do
    test "returns value of inserted key", c do
      btree =
        for n <- 1..20, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      for n <- 1..20 do
        assert "v-#{n}" == BTree.search(btree, n)
      end
    end

    test "returns nil if no key exists", c do
      assert nil == BTree.search(c.btree, "non-existing-key")
    end
  end

  describe "select/4" do
    test "returns stream over all leafs", c do
      btree =
        for n <- 1..20, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      info_f = fn ->
        btree
      end

      assert for(n <- 1..20, do: {n, "v-#{n}"}) ==
               BTree.select(info_f, nil, nil) |> Enum.to_list()
    end

    test "returns stream with leafs in range", c do
      btree =
        for n <- 1..20, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      info_f = fn ->
        btree
      end

      assert for(n <- 5..10, do: {n, "v-#{n}"}) ==
               BTree.select(info_f, 5, 10) |> Enum.to_list()
    end
  end

  describe "copy/3" do
    test "copies addressable tree from old log to new log", c do
      btree =
        for n <- 1..20, reduce: c.btree do
          acc ->
            BTree.add(acc, n, "v-#{n}")
        end

      {:ok, %{size: size}} = File.stat(c.btree.log.file_path)

      assert %BTree{} = btree = BTree.copy(btree, c.tmp_dir)

      {:ok, %{size: new_size}} = File.stat(btree.log.file_path)

      assert new_size < size

      for n <- 1..20 do
        assert "v-#{n}" == BTree.search(btree, n)
      end
    end
  end
end
