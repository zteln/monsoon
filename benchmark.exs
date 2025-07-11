Mix.install([{:monsoon, path: "#{File.cwd!()}"}, :benchee])

# db_dir = "/tmp"
#
# {:ok, db} = Monsoon.start_link(dir: db_dir)
#
# for n <- 0..10000 do
#   k = "k-#{n}"
#   v = "v-#{n}"
#   Monsoon.put(db, k, v)
#   # Process.sleep(1000)
# end
#
# Monsoon.get(db, "k-75")
# |> IO.inspect()
# Setup
db_dir = "/tmp/monsoon_bench"
File.rm_rf(db_dir)
File.mkdir_p!(db_dir)

# Helper functions
defmodule BenchmarkHelpers do
  def setup_db(dir) do
    name = :"db_#{System.unique_integer([:positive])}"
    {:ok, db} = Monsoon.start_link(dir: dir, name: name)
    db
  end

  def generate_data(count) do
    1..count
    |> Enum.map(fn n -> {"key_#{n}", "value_#{String.duplicate("x", 100)}_#{n}"} end)
  end

  def populate_db(db, data) do
    Monsoon.start_transaction(db)
    Enum.each(data, fn {k, v} -> Monsoon.put(db, k, v) end)
    Monsoon.end_transaction(db)
  end
end

# Benchmark configurations
small_dataset = BenchmarkHelpers.generate_data(100)
medium_dataset = BenchmarkHelpers.generate_data(1_000)
large_dataset = BenchmarkHelpers.generate_data(10_000)
huge_dataset = BenchmarkHelpers.generate_data(100_000)

Benchee.run(
  %{
    "put_single" => fn {db, {k, v}} ->
      Monsoon.put(db, k, v)
      db
    end,
    "get_existing" => fn {db, {k, _v}} ->
      Monsoon.get(db, k)
      db
    end,
    # # "get_missing" => fn {db, _} ->
    # #   Monsoon.get(db, "missing_key")
    # #   db
    # # end,
    "select_all" => fn {db, _} ->
      Monsoon.select(db)
      db
    end,
    "select_range" => fn {db, _} ->
      Monsoon.select(db, "key_1", "key_99")
      db
    end
    # "transaction_put" => fn {db, data} ->
    #   Monsoon.start_transaction(db)
    #   Enum.each(data, fn {k, v} -> Monsoon.put(db, k, v) end)
    #   Monsoon.end_transaction(db)
    #   db
    # end
  },
  inputs: %{
    "small (100 items)" => {"small", small_dataset},
    "medium (1K items)" => {"medium", medium_dataset},
    "large (10K items)" => {"large", large_dataset},
    "huge (100K items)" => {"huge", huge_dataset}
  },
  before_each: fn {size, data} ->
    File.mkdir_p!("#{db_dir}/#{size}")
    db = BenchmarkHelpers.setup_db("#{db_dir}/#{size}")

    # Pre-populate db
    if size != "transaction_put" do
      BenchmarkHelpers.populate_db(db, Enum.take(data, length(data) - 1))
    end

    {db, Enum.at(data, -1)}
  end,
  after_each: fn db ->
    %{log: log} = :sys.get_state(db)
    Agent.stop(log.pid)
    GenServer.stop(db)
    File.rm_rf(db_dir)
  end,
  memory_time: 2,
  time: 5
  # profile_after: true
)

# Cleanup
File.rm_rf(db_dir)
