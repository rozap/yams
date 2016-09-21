defmodule QueryTest do
  use ExUnit.Case
  require Yams.Query
  alias Yams.Query
  alias Yams.Query.Aggregate
  import TestHelpers

  setup do
    Yams.start_link
    stream = make_yam_stream
    {:ok, %{stream: stream}}
  end

  test "can get the count the buckets", %{stream: stream} do
    mins = stream
    |> Query.bucket(10, "milliseconds")
    |> Query.count("row.num", "count_num")
    |> Query.aggregates
    |> Query.as_stream!
    |> Stream.map(fn %Aggregate{aggregations: %{"count_num" => mn}} -> mn end)
    |> Enum.into([])

    assert mins == [10, 10, 1]
  end

  # test "can count where a predicate is true the buckets", %{stream: stream} do
  #   mins = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.count_where("row.num" > 30 && "row.num" < 33, "count_num")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"count_num" => mn}} -> mn end)
  #   |> Enum.into([])

  #   assert mins == [2, 0, 0]
  # end

  # test "can get the minimum per bucket", %{stream: stream} do
  #   mins = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.minimum("row.num", "min_num")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"min_num" => mn}} -> mn end)
  #   |> Enum.into([])

  #   assert mins == [30, 40, 50]
  # end

  # test "can get the maximum per bucket", %{stream: stream} do
  #   maxes = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.maximum("row.num", "max_num")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"max_num" => mn}} -> mn end)
  #   |> Enum.into([])

  #   assert maxes == [39, 49, 50]
  # end

  # test "can get the percentile per bucket", %{stream: stream} do
  #   ps = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.percentile("row.num", 80, "thing")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"thing" => mn}} -> mn end)
  #   |> Enum.into([])

  #   assert ps == [37.2, 47.2, 50]
  # end

  # test "can get the percentile of an expr per bucket", %{stream: stream} do
  #   ps = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.percentile("row.num" / "row.num", 80, "throughput")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"throughput" => tp}} -> tp end)
  #   |> Enum.into([])

  #   assert ps == [1.0, 1.0, 1.0]
  # end

  # test "can get the max of an expr per bucket", %{stream: stream} do
  #   ps = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.maximum("row.num" - 50, "max_num")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: %{"max_num" => tp}} -> tp end)
  #   |> Enum.into([])

  #   assert ps == [-11, -1, 0]
  # end

  # test "can compose aggregations", %{stream: stream} do
  #   aggs = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.percentile("row.num", 80, "p80_num")
  #   |> Query.maximum("row.num", "max_num")
  #   |> Query.minimum("row.num", "min_num")
  #   |> Query.percentile("row.num", 99, "p99_num")
  #   |> Query.aggregates
  #   |> Query.as_stream!
  #   |> Stream.map(fn %Aggregate{aggregations: a} -> a end)
  #   |> Enum.into([])

  #   assert aggs == [
  #     %{"max_num" => 39, "min_num" => 30, "p80_num" => 37.2, "p99_num" => 38.91},
  #     %{"max_num" => 49, "min_num" => 40, "p80_num" => 47.2, "p99_num" => 48.91},
  #     %{"max_num" => 50, "min_num" => 50, "p80_num" => 50, "p99_num" => 50}
  #   ]
  # end

  # test "can filter raw things in a bucket", %{stream: stream} do
  #   nums = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.where("row.num" > 32 && "row.num" < 36)
  #   |> Query.as_stream!
  #   |> Stream.map(fn bucket -> Enum.map(bucket.data, fn {_, d} -> d["num"] end) end)
  #   |> Enum.into([])
  #   |> List.flatten

  #   assert nums == [33, 34, 35]
  # end

  # test "can filter aggregates", %{stream: stream} do
  #   [agg] = stream
  #   |> Query.bucket(10, "milliseconds")
  #   |> Query.percentile("row.num", 80, "p80_num")
  #   |> Query.aggregates
  #   |> Query.where("row.p80_num" > 37 && "row.p80_num" < 38)
  #   |> Query.as_stream!
  #   |> Stream.map(fn a -> a.aggregations["p80_num"] end)
  #   |> Enum.into([])

  #   assert agg == 37.2
  # end


end