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
      %{log: log, root_cursor: root_cursor} = :sys.get_state(c.db)

      assert {:ok, %BTree{is_leaf: true, pairs: [], children: []}} =
               Log.get_node(log, root_cursor)
    end
  end

  describe "put/3" do
    test "writes key-value to b-tree", c do
      assert :ok == Monsoon.put(c.db, :k, :v)

      %{log: log, root_cursor: root_cursor} = :sys.get_state(c.db)

      assert {:ok, %BTree{is_leaf: true, pairs: [{:k, :v}], children: []}} =
               Log.get_node(log, root_cursor)
    end
  end

  describe "get/2" do
    test "gets written key", c do
      assert :ok == Monsoon.put(c.db, :k, :v)
      assert {:ok, :v} == Monsoon.get(c.db, :k)
    end
  end

  describe "transaction" do
    test "start_transaction/1 can read inserted pairs", c do
      assert :ok == Monsoon.start_transaction(c.db)
      assert :ok == Monsoon.put(c.db, :k, :v)
      assert {:ok, :v} == Monsoon.get(c.db, :k)
      assert :ok == Monsoon.end_transaction(c.db)
      assert {:ok, :v} == Monsoon.get(c.db, :k)
    end

    test "cancel_transaction cannot read pairs inserted during transaction", c do
      assert :ok == Monsoon.start_transaction(c.db)
      assert :ok == Monsoon.put(c.db, :k, :v)
      assert {:ok, :v} == Monsoon.get(c.db, :k)
      assert :ok == Monsoon.cancel_transaction(c.db)
      assert {:error, nil} == Monsoon.get(c.db, :k)
    end
  end

  describe "cleans file" do
    test "when gen_limit is reached", c do
      pairs =
        for n <- 0..10 do
          {n, :"v#{n}"}
        end

      Enum.each(pairs, fn {k, v} ->
        assert :ok == Monsoon.put(c.db, k, v)
      end)

      Enum.each(pairs, fn {k, v} ->
        assert {:ok, v} == Monsoon.get(c.db, k)
      end)
    end
  end

  describe "remove/2" do
    setup c do
      data = gen_data()
      populate_db(c.db, data)
      %{data: data}
    end

    test "key in non-minimal leaf node is removed", c do
      assert :ok == Monsoon.remove(c.db, length(c.data))

      for n <- 1..(length(c.data) - 1) do
        assert {:ok, :"v-#{n}"} == Monsoon.get(c.db, n)
      end

      assert {:error, nil} == Monsoon.get(c.db, length(c.data))
    end

    test "key in minimal leaf node causes underflow", c do
      assert :ok == Monsoon.remove(c.db, 7)

      for n <- 1..length(c.data) do
        if n == 7 do
          assert {:error, nil} == Monsoon.get(c.db, n)
        else
          assert {:ok, :"v-#{n}"} == Monsoon.get(c.db, n)
        end
      end
    end
  end

  defp populate_db(db, data) do
    Monsoon.start_transaction(db)

    Enum.each(data, fn {k, v} ->
      Monsoon.put(db, k, v)
    end)

    Monsoon.end_transaction(db)
  end

  defp gen_data(size \\ 11) do
    for n <- 1..size do
      {n, :"v-#{n}"}
    end
  end
end
