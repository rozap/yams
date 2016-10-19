defmodule YamsTest do
  use ExUnit.Case
  alias Yams.{Session, Query}

  setup do
    Yams.start_link
    :ok
  end

  test "can put and get" do
    from_ts = Yams.key

    {:created, h} = Session.open(UUID.uuid1())

    :ok = Session.put(h, [:a, :b, :c])
    :ok = Session.put(h, [:d, :e, :f])
    :ok = Session.put(h, [:g, :h, :i])

    to_ts = Yams.key

    values = Session.stream!(h, {from_ts, to_ts})
    |> Query.as_stream!
    |> Stream.map(fn {_, v} -> v end)
    |> Enum.into([])

    assert values == [
      [:a, :b, :c],
      [:d, :e, :f],
      [:g, :h, :i]
    ]
  end

  test "can get a changes stream, put to it and get rows" do

    ref = UUID.uuid1()

    task = Task.async(fn ->
      {_, pid} = Session.open(ref)
      Session.changes!(pid)
      |> Query.as_stream!
      |> Stream.map(fn {_, v} -> v end)
      |> Enum.into([])
    end)

    {_, h} = Session.open(ref)

    from_ts = Yams.key()
    :ok = Session.put(h, [:a, :b, :c])
    :ok = Session.put(h, [:d, :e, :f])
    :ok = Session.put(h, [:g, :h, :i])
    to_ts = Yams.key()


    send task.pid, :done
    changes = Task.await(task)

    assert changes == [
      [:a, :b, :c],
      [:d, :e, :f],
      [:g, :h, :i]
    ]

    values = Session.stream!(h, {from_ts, to_ts})
    |> Query.as_stream!
    |> Stream.map(fn {_, v} -> v end)
    |> Enum.into([])

    assert values == changes
  end

  test "can shut down a changes stream" do

    ref = UUID.uuid1()

    task = Task.async(fn ->
      {_, pid} = Session.open(ref)
      Session.changes!(pid)
      |> Query.as_stream!
      |> Stream.map(fn {_, v} -> v end)
      |> Enum.into([])
    end)

    {_, h} = Session.open(ref)

    assert Session.listeners(h) == {:ok, 1}

    send task.pid, :done
    Task.await(task)

    :ok = Session.put(h, [:a, :b, :c])
    :ok = Session.put(h, [:d, :e, :f])
    :ok = Session.put(h, [:g, :h, :i])

    assert Session.listeners(h) == {:ok, 0}
  end
end