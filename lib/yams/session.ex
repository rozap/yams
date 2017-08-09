defmodule Yams.Session do
  use GenServer
  alias Yams.Query

  def init([key: key]) do
    {:ok, ref} = Application.get_env(:yams, :data)
    |> Path.join(key)
    |> :binary.bin_to_list
    |> :eleveldb.open([create_if_missing: true])

    {:ok, %{
      db: ref,
      subscribers: []
    }}
  end

  def key_to_string(key) do
    key |> to_string |> String.pad_leading(19, "0")
  end

  def handle_call({:put, {key, value}}, _, state) do
    serialized = :erlang.term_to_binary(value)
    result = :eleveldb.put(state.db, key_to_string(key), serialized, [])

    Enum.each(state.subscribers, fn {who, ref} ->
      send who, {:change, {key, value}, {:from, self, ref}}
    end)

    {:reply, result, state}
  end

  def handle_call({:stream, %Query.State{tstart: tstart, tend: tend} = query}, _, state) do
    starting = key_to_string(tstart)
    ending = key_to_string(tend)

    stream = Stream.resource(
      fn ->
        {:ok, it} = :eleveldb.iterator(state.db, [])
        {it, query, starting}
      end,
      fn {it, query, move_to} ->
        case :eleveldb.iterator_move(it, move_to) do
          {:ok, key, bin} ->
            if key <= ending do
              value = :erlang.binary_to_term(bin)
              {:ok, row, query} = Query.row(
                query,
                String.to_integer(key),
                value
              )

              {row, {it, query, :next}}
            else
              {:halt, {it, query, :done}}
            end
          {:error, _} = e ->
            {:halt, {it, query, :done}}
        end
      end,
      fn {it, _, _} -> :eleveldb.iterator_close(it) end
    )

    {:reply, {:ok, stream}, state}
  end

  def handle_call({:changes, who, ref}, _, state) do
    Process.monitor(who)
    {:reply, :ok, Map.put(state, :subscribers, [{who, ref} | state.subscribers])}
  end

  def handle_call(:listeners, _, state) do
    {:reply, {:ok, length(state.subscribers)}, state}
  end

  def handle_info({:DOWN, _ref, :process, who, _reason}, state) do
    subs = Enum.reject(state.subscribers, fn {sub, _} -> sub == who end)
    {:noreply, Map.put(state, :subscribers, subs)}
  end

  def stream!(pid, query) do
    {:ok, s} = GenServer.call(pid, {:stream, query})
    s
  end

  def put(pid, key, value) do
    GenServer.call(pid, {:put, {key, value}})
  end

  def put(pid, value) do
    put(pid, Yams.key, value)
  end


  def listeners(pid) do
    GenServer.call(pid, :listeners)
  end

  def changes!(pid) do
    start_t = Yams.key
    ref = make_ref
    stream = Stream.resource(
      fn ->
        GenServer.call(pid, {:changes, self, ref})
      end,
      fn state ->
        receive do
          {:change, row, {:from, ^pid, ^ref}} -> {[row], state}
          :done -> {:halt, state}
        end
      end,
      fn _ -> :ok end
    )

    # %Query.State{range: {start_t, :none}, stream: stream}
  end

  def subscribe_changes(pid) do
    GenServer.call(pid, {:changes, self})
  end

  def open(key) do
    name = String.to_atom("yams_session_#{key}")
    case GenServer.start(__MODULE__, [key: key], [name: name]) do
      {:error, {:already_started, pid}} -> {:existing, pid}
      {:ok, pid}                        -> {:created, pid}
      err                               -> err
    end
  end
end