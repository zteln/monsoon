Mix.install([{:monsoon, path: "#{File.cwd!()}"}, :benchee])

benchmark_tag = DateTime.utc_now(:second) |> DateTime.to_iso8601()

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

    Enum.each(data, fn {k, v} ->
      Monsoon.put(db, k, v)
    end)

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
      :ok = Monsoon.put(db, k, v)
      db
    end
  },
  inputs: %{
    "small (100 items)" => {"small", small_dataset},
    "medium (1K items)" => {"medium", medium_dataset},
    "large (10K items)" => {"large", large_dataset},
    "huge (100K items)" => {"huge", huge_dataset}
  },
  before_each: fn {size, data} ->
    # Ensure no previous data
    File.mkdir_p!("#{db_dir}/#{size}")

    # Start db
    db = BenchmarkHelpers.setup_db("#{db_dir}/#{size}")

    # Pre-populate db
    BenchmarkHelpers.populate_db(db, Enum.take(data, length(data) - 1))

    {db, Enum.at(data, -1)}
  end,
  after_each: fn db ->
    %{log: log} = :sys.get_state(db)
    Agent.stop(log.pid)
    GenServer.stop(db)
    File.rm_rf(db_dir)
  end,
  memory_time: 2,
  time: 5,
  profile_after: true
  # save: %{path: "benchmarks/#{benchmark_tag}.benchee", tag: benchmark_tag},
  # load: ["benchmarks/*"],
  # formatters: [
  #   {Benchee.Formatters.Console, comparison: true}
  # ]
)

# Cleanup
File.rm_rf(db_dir)
