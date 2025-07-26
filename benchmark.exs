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
    |> Enum.shuffle()
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

# Benchee.run(
#   %{
#     "put" => fn {db, %{put: {k, v}} = info} ->
#       :ok = Monsoon.put(db, k, v)
#       {db, info}
#     end,
#     "get" => fn {db, %{get: {k, _}} = info} ->
#       Monsoon.get(db, k)
#       {db, info}
#     end
#     # "remove" => fn {db, %{del: {k, _}} = info} ->
#     #   Monsoon.remove(db, k)
#     #   {db, info}
#     # end
#   },
#   inputs: %{
#     "small (100 items)" => {"small", small_dataset},
#     "medium (1K items)" => {"medium", medium_dataset},
#     "large (10K items)" => {"large", large_dataset},
#     "huge (100K items)" => {"huge", huge_dataset}
#   },
#   before_scenario: fn {size, data} ->
#     File.mkdir_p!("#{db_dir}/#{size}")
#     db = BenchmarkHelpers.setup_db("#{db_dir}/#{size}")
#     BenchmarkHelpers.populate_db(db, data)
#     {db, data}
#   end,
#   before_each: fn {db, data} ->
#     {put_k, put_v} = Enum.random(data)
#     {get_k, get_v} = Enum.random(data)
#     {del_k, del_v} = Enum.random(data)
#     Monsoon.remove(db, put_k)
#
#     info = %{
#       put: {put_k, put_v},
#       get: {get_k, get_v},
#       del: {del_k, del_v}
#     }
#
#     {db, info}
#   end,
#   # after_each: fn {db, %{del: {k, v}}} = arg ->
#   #   Monsoon.put(db, k, v)
#   #   arg
#   # end,
#   after_scenario: fn {db, _} ->
#     %{btree: %{log: log}} = :sys.get_state(db)
#     Agent.stop(log.pid)
#     GenServer.stop(db)
#     File.rm_rf(db_dir)
#   end,
#   memory_time: 2,
#   time: 5
#   # profile_after: :tprof
#   # save: %{path: "benchmarks/#{benchmark_tag}.benchee", tag: benchmark_tag},
#   # load: ["benchmarks/*"],
#   # formatters: [
#   #   {Benchee.Formatters.Console, comparison: true}
#   # ]
# )

# Cleanup
File.rm_rf(db_dir)
