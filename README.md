# Yams
yams is a small library for putting timeseries data in leveldb and getting it out in a streamy way.

<img src="http://i.imgur.com/CvQnZ9C.png" width="200"/>

### Make a leveldb timeseries store
```elixir
alias Yams.{Session, Query}

{:created, sesh} = Yams.Session.open("my_cool_db")

start_key = Yams.key
Enum.each(30..50, fn i ->
  Yams.Session.put(sesh, Yams.key, %{num: i})
end)
end_key = Yams.key

```

### Stream events out of the store with `stream!/2`
```elixir
Session.stream!(sesh, {start_key, end_key})
|> Query.bucket(10, "milliseconds")
|> Query.percentile("row.num", 80, "p80_num")
|> Query.maximum("row.num", "max_num")
|> Query.minimum("row.num", "min_num")
|> Query.percentile("row.num", 99, "p99_num")
|> Query.count_where("row.num" > 30 && "row.num" < 33, "num_between_30_and_33")
|> Query.aggregates
|> Query.as_stream!
|> Enum.into([])

# You might get something like:
[
  %Aggregate{aggregations: %{
      "max_num" => 39,
      "min_num" => 30,
      "num_between_30_and_33" => 2,
      "p80_num" => 37.2,
      "p99_num" => 38.91
    },
    end_t: 1472175016306554068,
    start_t: 1472175016297554068
  },
  %Aggregate{aggregations: %{
      "max_num" => 49,
      "min_num" => 40,
      "num_between_30_and_33" => 0,
      "p80_num" => 47.2, "p99_num" => 48.91
    },
    end_t: 1472175016316554068,
    start_t: 1472175016307554068},
  %Aggregate{aggregations: %{
      "max_num" => 50,
      "min_num" => 50,
      "num_between_30_and_33" => 0,
      "p80_num" => 50,
      "p99_num" => 50
    },
    end_t: 1472175016317554068,
    start_t: 1472175016317554068
  }
]

```

### Get a unterminated stream of changes with `changes!/1`

```elixir
Session.changes!(sesh)
|> Query.bucket(10, "milliseconds")
|> Query.percentile("row.num", 80, "p80_num")
|> Query.maximum("row.num", "max_num")
|> Query.minimum("row.num", "min_num")
|> Query.percentile("row.num", 99, "p99_num")
|> Query.count_where("row.num" > 30 && "row.num" < 33, "num_between_30_and_33")
|> Query.aggregates
|> Query.as_stream!
|> Stream.each(fn event -> IO.inspect event end) # This will print each event to the console as it comes in
|> Stream.run
```
