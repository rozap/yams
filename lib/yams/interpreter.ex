defmodule Yams.Interpreter do
  require Logger

  def error_or_subexprs(sexprs) do
    ast = Enum.map(sexprs, &to_expr/1)

    error = Enum.find(ast, fn
      {:error, _} -> true
      _ -> false
    end)

    if error do
      error
    else
      {:ok, ast}
    end
  end

  @queryers [
    "bucket",
    "minimum",
    "maximum",
    "count_where",
    "percentile",
    "where",
    "aggregates"
  ]

  Enum.each(@queryers, fn func_name ->
    defp to_expr([unquote(func_name) | args]) do
      with {:ok, subexprs} <- error_or_subexprs(args) do
        {
          {:., [], [
            {:__aliases__, [], [:Yams, :Query]},
            String.to_atom(unquote(func_name))
          ]},
          [],
          subexprs
        }
      end
    end
  end)

  @operators [
    ">",
    ">=",
    "<",
    "<=",
    "==",
    "!=",
    "&&",
    "||",
    "+",
    "-",
    "*",
    "/",
  ]

  Enum.each(@operators, fn op ->
    defp to_expr([unquote(op) | args]) do
      with {:ok, subexprs} <- error_or_subexprs(args) do
        {
          String.to_atom(unquote(op)),
          [],
          subexprs
        }
      end
    end
  end)

  defp to_expr(prim) when is_integer(prim), do: prim
  defp to_expr(prim) when is_float(prim),   do: prim
  defp to_expr(prim) when is_binary(prim),  do: prim

  defp to_expr([funcall | args]) do
    {:error, "Unknown function call (#{funcall} #{Enum.join(args, " ")})"}
  end
  defp to_expr(prim) do
    {:error, "Wanted a primitive, got #{inspect prim}"}
  end


  defp inject_yam_stream({func_name, meta, args}, ys) do
    {func_name, meta, [ys | args]}
  end

  defp compile_expr(sexpr, upstream) do
    case to_expr(sexpr) do
      {:error, _} = e -> e
      ast -> {:ok, inject_yam_stream(ast, upstream)}
    end
  end

  def compile(pipeline) do
    result = Enum.reduce_while(
      pipeline,
      {:ok, Macro.var(:yam_stream, nil)},
      fn (sexpr, {:ok, upstream}) ->
        case compile_expr(sexpr, upstream) do
          {:error, _} = e -> {:halt, e}
          {:ok, _} = ok   -> {:cont, ok}
        end
      end
    )

    with {:ok, compiled} <- result do
      quoted = quote do
        require Yams.Query
        fn s ->
          var!(yam_stream) = s
          unquote(compiled)
        end
      end

      {:ok, quoted}
    end

  end

  def run(stream, pipeline) do
    with {:ok, quoted} <- compile(pipeline) do
      {func, _} = Code.eval_quoted(quoted)
      try do
        {:ok, func.(stream)}
      rescue
        e in [UndefinedFunctionError] ->
          Logger.warn("failed to interpret #{inspect pipeline} #{inspect e}")
          message = "Undefined function #{e.function}/#{e.arity}"
          {:error, message}
        e ->
          {:error, e.message}
      end
    end
  end
end