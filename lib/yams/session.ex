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

  def handle_call({:put, {key, value}}, _, state) do
    serialized = :erlang.term_to_binary(value)
    result = :eleveldb.put(state.db, "#{key}", serialized, [])

    Enum.each(state.subscribers, fn {who, ref} ->
      send who, {:change, {key, value}, {:from, self, ref}}
    end)

    {:reply, result, state}
  end

  def handle_call({:stream, {_, end_time}}, _, state) do
    stream = Stream.resource(
      fn ->
        {:ok, ref} = :eleveldb.iterator(state.db, [])
        {:first, ref}
      end,
      fn {state, ref} ->
        case :eleveldb.iterator_move(ref, state) do
          {:ok, key, bin} ->
            t = String.to_integer(key)
            if t <= end_time do
              value = :erlang.binary_to_term(bin)

              {[{t, value}], {:next, ref}}
            else
              {:halt, {:done, ref}}
            end
          {:error, _} ->
            {:halt, {:done, ref}}
        end
      end,
      fn {_, ref} -> :eleveldb.iterator_close(ref) end
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

  def stream!(pid, range) do
    case GenServer.call(pid, {:stream, range}) do
      {:ok, stream} -> %Query.State{range: range, stream: stream}
      err -> err
    end
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

    %Query.State{range: {start_t, :none}, stream: stream}
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