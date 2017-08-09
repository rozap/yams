defmodule Yams.Interpreter do
  require Yams.Interpreter.Operator
  import Yams.Interpreter.Operator

  def init_accumulator([_funcname | args]) do
    Enum.map(args, fn arg -> init_accumulator(arg) end)
  end
  def init_accumulator(_constant), do: []

  defoperator :+
  defoperator :-

  def eval([:attribute, name], row, [[]] = acc) do
    {Map.get(row, name), acc}
  end

end