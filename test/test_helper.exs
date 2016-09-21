defmodule TestHelpers do

  def from_ts do
    1472175016297554068
  end

  def to_ts do
    from_ts + Yams.ms_to_key(20)
  end

  def make_yam_stream(ref \\ nil, count \\ 30) do
    {:created, h} = Yams.Session.open(ref || UUID.uuid1())

    start_t = 30
    end_t = start_t + count

    Enum.each(start_t..end_t, fn num ->
      t = num - start_t
      key = from_ts + Yams.ms_to_key(t)
      :ok = Yams.Session.put(h, key, %{
        "num" => num,
        "str" => "foo_#{num}",
        "start_t" => Yams.key_to_ms(from_ts),
        "end_t" => Yams.key_to_ms(from_ts) + num,
        "size" => num * num * num
      })
    end)
    range = {from_ts, to_ts}

    Yams.Session.stream!(h, range)
  end


  defmacro timer(body) do
    quote do
      start_time = System.os_time(:milliseconds)
      result = unquote(body[:do])
      end_time = System.os_time(:milliseconds)
      {end_time - start_time, result}
    end
  end

end

ExUnit.start(timeout: 5_000)
