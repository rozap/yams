defmodule Yams.Query do
  alias Yams.Interpreter

  defmodule State do
    @enforce_keys [:tstart, :tend]
    defstruct [
      :tstart,
      :tend,
      exprs: [],
      accumulation: %{}
    ]
  end


  defp reduce_expr([:annotate, label, expr], row, {attrs, acc}) do
    {value, expr_acc} = Interpreter.eval(
      expr,
      row,
      Interpreter.init_accumulator(expr)
    )

    {[{label, value} | attrs], Map.put(acc, label, expr_acc)}
  end


  def row(%State{exprs: exprs} = state, key, row) do
    {row, accumulation} = Enum.reduce(
      exprs,
      {[], state.accumulation},
      fn expr, acc -> reduce_expr(expr, row, acc) end
    )

    {:ok, [{key, row}], %{state | accumulation: accumulation}}
  end
end