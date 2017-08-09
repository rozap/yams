defmodule InterpreterTest do
  use ExUnit.Case
  alias Yams.{Session, Query}

  setup do
    Yams.start_link

    {:created, h} = Session.open(UUID.uuid1())

    Enum.map(0..10, fn i ->
      Session.put(h, i, %{
        "high" => i * 3,
        "low" => i,
        "close" => i * 2}
      )
    end)

    {:ok, %{h: h}}
  end

  test "can evaluate attribute", %{h: h} do
    assert h
    |> Session.stream!(%Query.State{tstart: 2, tend: 4, exprs: [
      [:annotate, "high", [:attribute, "high"]]
    ]})
    |> Enum.into([]) == [
      {2, [{"high", 6}]},
      {3, [{"high", 9}]},
      {4, [{"high", 12}]}
    ]
  end

  test "can evaluate many attributes", %{h: h} do
    assert h
    |> Session.stream!(%Query.State{tstart: 2, tend: 4, exprs: [
      [:annotate, "high", [:attribute, "high"]],
      [:annotate, "low", [:attribute, "low"]],
      [:annotate, "close", [:attribute, "close"]]

    ]})
    |> Enum.into([]) == [
      {2, [{"close", 4}, {"low", 2}, {"high", 6}]},
      {3, [{"close", 6}, {"low", 3}, {"high", 9}]},
      {4, [{"close", 8}, {"low", 4}, {"high", 12}]}
    ]
  end

  test "can evaluate plus", %{h: h} do
    assert h
    |> Session.stream!(%Query.State{tstart: 2, tend: 4, exprs: [
      [:annotate, "low+hi", [:+,
        [:attribute, "low"],
        [:attribute, "high"]
      ]],
    ]})
    |> Enum.into([]) == [
      {2, [{"low+hi", 8}]},
      {3, [{"low+hi", 12}]},
      {4, [{"low+hi", 16}]}
    ]

    assert h
    |> Session.stream!(%Query.State{tstart: 2, tend: 4, exprs: [
      [:annotate, "low+1", [:+,
        [:attribute, "low"],
        1
      ]],
    ]})
    |> Enum.into([]) == [
      {2, [{"low+1", 3}]},
      {3, [{"low+1", 4}]},
      {4, [{"low+1", 5}]}
    ]
  end

  test "can evaluate minus", %{h: h} do
    assert h
    |> Session.stream!(%Query.State{tstart: 2, tend: 4, exprs: [
      [:annotate, "hi-lo", [:-,
        [:attribute, "high"],
        [:attribute, "low"]
      ]],
    ]})
    |> Enum.into([]) == [
      {2, [{"hi-lo", 4}]},
      {3, [{"hi-lo", 6}]},
      {4, [{"hi-lo", 8}]}
    ]
  end

end
