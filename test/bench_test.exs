defmodule BenchTest do
  use ExUnit.Case
  require Yams.Query
  alias Yams.Query
  alias Yams.Query.Aggregate
  import TestHelpers

  @from_ts 1472175016297554068
  @count 10_000

  def keyspace do
    {@from_ts, @from_ts + Yams.ms_to_key(@count)}
  end

  setup_all do
    ref = UUID.uuid1()

    {:created, h} = Yams.Session.open(ref)

    Enum.each(0..@count, fn num ->
      key = @from_ts + Yams.ms_to_key(num)
      :ok = Yams.Session.put(h, key, %{
        "num" => num,
        "str" => "foo_#{num}"
      })
    end)

    {:ok, %{ref: ref}}
  end

  defp open_stream(ref) do
    {_, sesh} = Yams.Session.open(ref)
    Yams.Session.stream!(sesh, keyspace())
  end

  test "100ms bucket count perf", %{ref: ref} do
    {elapsed, _} = timer do
      mins = open_stream(ref)
      |> Query.bucket(100, "milliseconds")
      |> Query.count("row.num", "a")
      |> Query.aggregates
      |> Query.as_stream!
      |> Stream.map(fn %Aggregate{aggregations: %{"a" => mn}} -> mn end)
      |> Enum.into([])
    end

    IO.puts "Counted #{@count} in #{elapsed}ms for #{trunc(1000 * (@count / elapsed))} row/s"
  end

  test "100ms bucket percentile perf", %{ref: ref} do
    {elapsed, result} = timer do
      mins = open_stream(ref)
      |> Query.bucket(100, "milliseconds")
      |> Query.percentile("row.num", 90, "a")
      |> Query.aggregates
      |> Query.as_stream!
      |> Stream.map(fn %Aggregate{aggregations: %{"a" => mn}} -> mn end)
      |> Enum.into([])
    end

    IO.puts "Counted #{@count} in #{elapsed}ms for #{trunc(1000 * (@count / elapsed))} row/s"
  end

  test "100ms bucket count_where perf", %{ref: ref} do
    {elapsed, _} = timer do
      mins = open_stream(ref)
      |> Query.bucket(100, "milliseconds")
      |> Query.count_where("row.num" > 2 && "row.num" < 80, "a")
      |> Query.aggregates
      |> Query.as_stream!
      |> Stream.map(fn %Aggregate{aggregations: %{"a" => mn}} -> mn end)
      |> Enum.into([])
    end
    IO.puts "Counted #{@count} in #{elapsed}ms for #{trunc(1000 * (@count / elapsed))} row/s"
  end

end