defmodule MonsoonTest do
  use ExUnit.Case, async: true

  alias Monsoon.BTree
  alias Monsoon.Log

  @moduletag :tmp_dir

  setup c do
    pid = start_supervised!({Monsoon, dir: c.tmp_dir, capacity: 2, gen_limit: 5})
    %{db: pid}
  end

  describe "start_link/1" do
    test "writes empty root to file", c do
      %{btree: btree} = :sys.get_state(c.db)

      assert {:ok, %BTree.Leaf{keys: [], values: []}} =
               Log.get_node(btree.log, btree.root_bp)
    end
  end

  describe "add/3" do
    test "inserts into empty b-tree", c do
      assert :ok == Monsoon.put(c.db, :k, :v)
      assert :v == Monsoon.get(c.db, :k)
    end

    test "already added key updates value", c do
      assert :ok == Monsoon.put(c.db, :k, :v1)
      assert :v1 == Monsoon.get(c.db, :k)
      assert :ok == Monsoon.put(c.db, :k, :v2)
      assert :v2 == Monsoon.get(c.db, :k)
    end

    test "splits b-tree root", c do
      for n <- 0..4 do
        assert :ok == Monsoon.put(c.db, n, "A-#{n}")
      end

      for n <- 0..4 do
        assert "A-#{n}" == Monsoon.get(c.db, n)
      end
    end
  end

  describe "remove/2" do
    test "removes single entry in leaf root", c do
      Monsoon.put(c.db, :k, :v)
      assert :ok == Monsoon.remove(c.db, :k)
      assert nil == Monsoon.get(c.db, :k)
    end

    test "can remove non-existing key", c do
      assert :ok == Monsoon.remove(c.db, :non_existing_key)
    end

    # test "rotates nodes", c do
    #   for n <- 0..4 do
    #     Monsoon.put(c.db, n, :"v-#{n}")
    #   end
    #
    #   %{log: log, btree: {root_loc, _, _}} = :sys.get_state(c.db)
    #
    #   assert {:ok, %BTree.Interior{keys: [2]}} =
    #            Log.get_node(log, root_loc)
    #
    #   assert :ok == Monsoon.remove(c.db, 1)
    #
    #   %{log: log, btree: {root_loc, _, _}} = :sys.get_state(c.db)
    #
    #   assert {:ok, %BTree.Interior{keys: [3]}} =
    #            Log.get_node(log, root_loc)
    # end
    #
    # test "merges nodes", c do
    #   for n <- 0..3 do
    #     Monsoon.put(c.db, n, :"v-#{n}")
    #   end
    #
    #   %{log: log, btree: {root_loc, _, _}} = :sys.get_state(c.db)
    #
    #   assert {:ok, %BTree.Interior{keys: [2]}} =
    #            Log.get_node(log, root_loc)
    #
    #   assert :ok == Monsoon.remove(c.db, 1)
    #   assert :ok == Monsoon.remove(c.db, 2)
    #
    #   %{log: log, btree: {root_loc, _, _}} = :sys.get_state(c.db)
    #
    #   assert {:ok, %BTree.Leaf{keys: [0, 3], values: [:"v-0", :"v-3"]}} =
    #            Log.get_node(log, root_loc)
    # end
  end

  describe "search/2" do
    test "gets key in leaf", c do
      Monsoon.put(c.db, 1, "A")
      assert "A" == Monsoon.get(1)
      assert nil == Monsoon.get(2)
    end

    test "gets key when depth > 1", c do
      for n <- 0..10 do
        Monsoon.put(c.db, n, "A-#{n}")
      end

      for n <- 0..10 do
        assert "A-#{n}" == Monsoon.get(c.db, n)
      end
    end
  end

  describe "select/3" do
    setup c do
      for n <- 0..10 do
        Monsoon.put(c.db, n, "A-#{n}")
      end

      :ok
    end

    test "all keys", c do
      assert for(n <- 0..10, do: {n, "A-#{n}"}) == Monsoon.select(c.db) |> Enum.to_list()
    end

    test "a range", c do
      assert for(n <- 3..7, do: {n, "A-#{n}"}) == Monsoon.select(c.db, 3, 7) |> Enum.to_list()
    end

    test "returns the latest", c do
      stream = Monsoon.select(c.db)

      assert for(n <- 0..10, do: {n, "A-#{n}"}) == Enum.to_list(stream)

      Monsoon.put(c.db, 11, "A-#{11}")
      Monsoon.remove(c.db, 0)

      assert for(n <- 1..11, do: {n, "A-#{n}"}) == Enum.to_list(stream)
    end
  end

  describe "start_transaction/1,end_transaction/1,cancel_transaction/1" do
    test "transaction commits when done", c do
      assert :ok == Monsoon.start_transaction(c.db)
      assert :ok == Monsoon.put(c.db, 1, "A-1")
      assert :ok == Monsoon.end_transaction(c.db)
      assert "A-1" == Monsoon.get(c.db, 1)
    end

    test "transaction is process bound", c do
      ref = make_ref()
      pid = self()

      assert :ok == Monsoon.start_transaction(c.db)
      assert :ok == Monsoon.put(c.db, 1, "A-1")

      spawn(fn ->
        assert nil == Monsoon.get(c.db, 1)
        send(pid, {:done, ref})
      end)

      receive do
        {:done, ^ref} ->
          :ok
      end

      assert "A-1" == Monsoon.get(c.db, 1)
      assert :ok == Monsoon.end_transaction(c.db)
      assert "A-1" == Monsoon.get(c.db, 1)

      spawn(fn ->
        assert "A-1" == Monsoon.get(c.db, 1)
        send(pid, {:done, ref})
      end)

      receive do
        {:done, ^ref} ->
          :ok
      end
    end

    test "canceling a transaction does not update tree", c do
      assert :ok == Monsoon.start_transaction(c.db)
      assert :ok == Monsoon.put(c.db, 1, "A-1")
      assert "A-1" == Monsoon.get(c.db, 1)
      assert :ok = Monsoon.cancel_transaction(c.db)
      assert nil == Monsoon.get(c.db, 1)
    end

    test "only one transaction at a time", c do
      ref = make_ref()
      parent = self()
      assert :ok == Monsoon.start_transaction(c.db)

      spawn(fn ->
        assert {:error, :tx_occupied} == Monsoon.start_transaction(c.db)
        send(parent, {:done, ref})
      end)

      receive do
        {:done, ^ref} ->
          :ok
      end
    end

    test "transaction blocks writes", c do
      ref = make_ref()
      parent = self()
      assert :ok == Monsoon.start_transaction(c.db)

      spawn(fn ->
        assert {:error, :not_tx_proc} == Monsoon.put(c.db, 1, "v-1")
        send(parent, {:done, ref})
      end)

      receive do
        {:done, ^ref} ->
          :ok
      end
    end

    test "transaction does not block reads", c do
      ref = make_ref()
      parent = self()
      assert :ok == Monsoon.put(c.db, 1, "v-1")
      assert :ok == Monsoon.start_transaction(c.db)

      spawn(fn ->
        assert "v-1" == Monsoon.get(c.db, 1)
        send(parent, {:done, ref})
      end)

      receive do
        {:done, ^ref} ->
          :ok
      end
    end
  end
end
