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

  describe "transaction/2" do
    test "commits when done", c do
      assert :ok ==
               Monsoon.transaction(c.db, fn ->
                 Monsoon.put(c.db, 1, "v-1")
                 :done
               end)

      assert "v-1" == Monsoon.get(c.db, 1)
    end

    test "does not commit when cancelled", c do
      assert :ok ==
               Monsoon.transaction(c.db, fn ->
                 Monsoon.put(c.db, 1, "v-1")
                 :cancel
               end)

      assert nil == Monsoon.get(c.db, 1)
    end

    test "transactions are atomic", c do
      assert :ok == Monsoon.put(c.db, 1, "v-1")

      assert :ok ==
               Monsoon.transaction(fn ->
                 assert :ok == Monsoon.put(c.db, 1, "v-2")
                 assert :ok == Monsoon.put(c.db, 2, "v-2")
                 assert :ok == Monsoon.put(c.db, 3, "v-2")
                 assert :ok == Monsoon.put(c.db, 4, "v-2")
                 :cancel
               end)

      assert "v-1" == Monsoon.get(c.db, 1)
      assert nil == Monsoon.get(c.db, 2)
      assert nil == Monsoon.get(c.db, 3)
      assert nil == Monsoon.get(c.db, 4)
    end

    test "transactions are isolated", c do
      parent = self()

      assert :ok == Monsoon.put(c.db, 1, "v-1")

      pid =
        spawn(fn ->
          tx_pid =
            receive do
              {:start, tx_pid} ->
                tx_pid
            end

          assert "v-1" == Monsoon.get(c.db, 1)

          send(tx_pid, :done)
        end)

      assert :ok ==
               Monsoon.transaction(fn ->
                 send(pid, {:start, self()})

                 assert :ok == Monsoon.put(c.db, 1, "v-2")

                 receive do
                   :done -> :ok
                 end

                 send(parent, :done)

                 :done
               end)

      receive do
        :done ->
          :ok
      end

      assert "v-2" == Monsoon.get(c.db, 1)
    end

    test "readers are not blocked by transactions", c do
      parent = self()

      pid =
        spawn(fn ->
          tx_pid =
            receive do
              {:start, tx_pid} ->
                tx_pid
            end

          assert nil == Monsoon.get(c.db, 1)

          send(tx_pid, :done)
        end)

      assert :ok ==
               Monsoon.transaction(fn ->
                 send(pid, {:start, self()})

                 receive do
                   :done -> :ok
                 end

                 send(parent, :done)

                 :done
               end)

      receive do
        :done ->
          :ok
      end
    end

    test "writers are blocked until transaction finishes", c do
      parent = self()

      pid =
        spawn(fn ->
          receive do
            :start ->
              :ok
          end

          assert :ok == Monsoon.put(c.db, 1, "v-2")

          receive do
            :end ->
              send(parent, :cont)
          end
        end)

      assert :ok ==
               Monsoon.transaction(c.db, fn ->
                 send(pid, :start)
                 Monsoon.put(c.db, 1, "v-1")
                 send(pid, :end)
                 :done
               end)

      receive do
        :cont ->
          :ok
      end

      assert "v-2" == Monsoon.get(c.db, 1)
    end
  end
end
