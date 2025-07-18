defmodule Monsoon.LogTest do
  use ExUnit.Case, async: true
  alias Monsoon.Log
  alias Monsoon.BTree

  @moduletag :tmp_dir
  setup c do
    file_path = Path.join(c.tmp_dir, "log")
    {:ok, log} = Log.new(file_path)
    %{log: log, log_path: file_path}
  end

  describe "new/1" do
    test "has lock on resource", c do
      assert false == :global.set_lock({c.log_path, self()}, [node()], 0)
    end
  end

  describe "move/2" do
    test "renames and switches lock", c do
      log_path = c.log_path
      new_log_path = Path.join(c.tmp_dir, "new_log")
      {:ok, new_log} = Log.new(new_log_path)

      assert %Log{file_path: ^log_path} = Log.move(c.log, new_log)

      assert false == :global.set_lock({c.log_path, self()}, [node()], 0)
      assert true == :global.set_lock({new_log_path, self()}, [node()], 0)
    end
  end

  describe "commit/2" do
    test "writes commit to log", c do
      assert :ok == Log.commit(c.log, {{1, 5}, {2, 6}, {3, 7}})
      assert {:ok, {{1, 5}, {2, 6}, {3, 7}}} == Log.get_commit(c.log)
    end
  end

  describe "put_node/2" do
    test "writes node to log", c do
      leaf = BTree.Leaf.new()
      interior = %BTree.Interior{}
      assert leaf_bp = Log.put_node(c.log, leaf)
      assert interior_bp = Log.put_node(c.log, interior)
      :ok = Log.flush(c.log)
      assert {:ok, leaf} == Log.get_node(c.log, leaf_bp)
      assert {:ok, interior} == Log.get_node(c.log, interior_bp)
    end
  end

  describe "get_node_by_id/2" do
    setup c do
      leaf = BTree.Leaf.new()
      assert leaf_bp = Log.put_node(c.log, leaf)
      :ok = Log.flush(c.log)
      %{leaf: leaf, leaf_bp: leaf_bp}
    end

    test "gets written node", c do
      assert {:ok, c.leaf} == Log.get_node_by_id(c.log, c.leaf.id)
    end

    test "Fails to get node if invalid id", c do
      assert {:error, :not_found} == Log.get_node_by_id(c.log, <<123::integer-64>>)
    end
  end

  describe "get_node/2" do
    setup c do
      leaf = BTree.Leaf.new()
      assert {_loc, _size} = leaf_bp = Log.put_node(c.log, leaf)
      :ok = Log.flush(c.log)
      %{leaf: leaf, leaf_bp: leaf_bp}
    end

    test "gets written node", c do
      assert {:ok, c.leaf} == Log.get_node(c.log, c.leaf_bp)
    end

    test "does not get node with invalid block pointer", c do
      assert {:error, :unable_to_decode_node} == Log.get_node(c.log, {1, 1024})
    end
  end

  describe "put_leaf_links/2" do
    test "updates leaf links in file", c do
      leaf_links = %{1 => {2, 3}}

      assert {_loc, _size} = leaf_links_bp = Log.put_leaf_links(c.log, leaf_links)
      :ok = Log.flush(c.log)

      assert {:ok, %{1 => {2, 3}}} == Log.get_leaf_links(c.log, leaf_links_bp)
    end
  end
end
