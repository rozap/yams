defmodule Yams.Interpreter.Operator do

  defmacro defoperator(operator) do
    quote do
      def eval([unquote(operator), l, r], _, acc)
      when is_number(l) and is_number(r) do
        value = unquote({
          operator,
          [context: Elixir, import: Kernel],
          [
            Macro.var(:l, __MODULE__),
            Macro.var(:r, __MODULE__)
          ]
        })

        {value, acc}
      end

      def eval([unquote(operator), l, r], row, [l_acc, r_acc])
      when is_number(l) do
        {r, r_acc} = eval(r, row, r_acc)
        value = unquote({
          operator,
          [context: Elixir, import: Kernel],
          [
            Macro.var(:l, __MODULE__),
            Macro.var(:r, __MODULE__)
          ]
        })
        {value, [l_acc, r_acc]}
      end

      def eval([unquote(operator), l, r], row, [l_acc, r_acc])
      when is_number(r) do
        {l, l_acc} = eval(l, row, l_acc)
        value = unquote({
          operator,
          [context: Elixir, import: Kernel],
          [
            Macro.var(:l, __MODULE__),
            Macro.var(:r, __MODULE__)
          ]
        })
        {value, [l_acc, r_acc]}
      end

      def eval([unquote(operator), l, r], row, [l_acc, r_acc]) do
        {l, l_acc} = eval(l, row, l_acc)
        {r, r_acc} = eval(r, row, r_acc)
        value = unquote({
          operator,
          [context: Elixir, import: Kernel],
          [
            Macro.var(:l, __MODULE__),
            Macro.var(:r, __MODULE__)
          ]
        })
        {value, [l_acc, r_acc]}
      end
    end
  end


end