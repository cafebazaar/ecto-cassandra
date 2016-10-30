defmodule Cassandra.Ecto do

  alias Ecto.Query.BooleanExpr

  @identifier ~r/[a-zA-Z][a-zA-Z0-9_]*/
  @unquoted_name ~r/[a-zA-Z_0-9]{1,48}/
  @binary_operators_map %{
    :== => "=",
    :<  => "<",
    :>  => ">",
    :<= => "<=",
    :>= => ">=",
    :!= => "!=",
  }
  @binary_operators Map.keys(@binary_operators_map)

  ### API ###

  def to_cql(query, operation) do
    {query, _, _} = apply(__MODULE__, operation, [query, []])
    query
  end

  def all(%{sources: sources} = query, options \\ []) do
    query = assemble([
      select(query, sources),
      from(query, sources),
      where(query, sources),
      group_by(query, sources),
      order_by(query, sources),
      limit(query, sources),
      lock(query.lock),
    ])

    {query, [], options}
  end

  def delete_all(%{sources: sources} = query, options) do
    table = table_name(query)
    {query, values} = case where(query, sources) do
      nil ->
        {"TRUNCATE #{table}", []}
      where ->
        assemble_values([
          {"DELETE FROM #{table}", []},
          ifelse(options[:if] == :exists, "IF EXISTS", nil),
          using(options[:ttl], options[:timestamp]),
        ])
    end

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  def insert(prefix, source, fields, options) do
    {query, values} = assemble_values([
      "INSERT INTO",
      table_name(prefix, source),
      values(fields),
      ifelse(options[:if] == :not_exists, "IF NOT EXISTS", nil),
      using(options[:ttl], options[:timestamp]),
    ])

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  def delete(prefix, source, filters, options) do
    # TODO: support IF conditions

    {query, values} = assemble_values([
      "DELETE FROM",
      table_name(prefix, source),
      using(options[:ttl], options[:timestamp]),
      where(filters),
      ifelse(options[:if] == :exists, "IF EXISTS", nil),
    ])

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  ### Helpers ###

  defp values(fields) do
    {funcs, fields} = Enum.partition fields, fn
      {_, val} -> match?(%Cassandra.UUID{value: nil}, val)
    end
    {field_names, field_values} = Enum.unzip(fields)
    {func_names, func_values}   = Enum.unzip(funcs)

    func_values = Enum.map_join func_values, ", " , fn
      %Cassandra.UUID{type: :timeuuid, value: nil} -> "now()"
      %Cassandra.UUID{type: :uuid,     value: nil} -> "uuid()"
    end

    names = Enum.map_join(field_names ++ func_names, ", ", &identifier/1)

    marks = marks(Enum.count(field_names))
    embeded_values = if func_values == "" do
      marks
    else
      "#{marks}, #{func_values}"
    end

    {"(#{names}) VALUES (#{embeded_values})", field_values}
  end

  defp marks(n) do
    ["?"]
    |> Stream.cycle
    |> Enum.take(n)
    |> Enum.join(", ")
  end

  defp select(%{select: %{fields: fields}} = query, sources) do
    fields
    |> select_fields(sources, query)
    |> prepend("SELECT ")
  end

  defp from(query, _sources) do
    query
    |> table_name
    |> prepend("FROM ")
  end

  defp where(filters) when is_list(filters) do
    {fields, values} = Enum.unzip(filters)
    conditions = Enum.map_join(fields, " AND ", &"#{identifier(&1)} = ?")
    {"WHERE #{conditions}", values}
  end

  defp where(%{wheres: []}, _), do: nil
  defp where(%{wheres: wheres} = query, sources) do
    wheres
    |> boolean(sources, query)
    |> prepend("WHERE ")
  end

  # TODO: GROUP BY added in cassandra 3.10 and has a bad error or previous versions
  # Maybe we must warn user about cassandra version
  defp group_by(%{group_bys: []}, _), do: nil
  defp group_by(%{group_bys: group_bys} = query, sources) do
    group_bys
    |> Enum.flat_map(fn %{expr: expr} -> expr end)
    |> Enum.map_join(", ", &expr(&1, sources, query))
    |> prepend("GROUP BY ")
  end

  defp order_by(%{order_bys: []}, _), do: nil
  defp order_by(%{order_bys: order_bys} = query, sources) do
    order_bys
    |> Enum.flat_map(fn %{expr: expr} -> expr end)
    |> Enum.map_join(", ", &order_by_expr(&1, sources, query))
    |> prepend("ORDER BY ")
  end

  defp order_by_expr({dir, expr}, sources, query) do
    expr(expr, sources, query) <> ifelse(dir == :desc, " DESC", "")
  end

  defp limit(%{limit: nil}, _sources), do: nil
  defp limit(%{limit: %{expr: expr}} = query, sources) do
    "LIMIT " <> expr(expr, sources, query)
  end

  defp lock(nil), do: nil
  defp lock("ALLOW FILTERING"), do: "ALLOW FILTERING"
  defp lock(_), do: raise ArgumentError, "Cassandra do not support locking"

  defp using(nil, nil),       do: nil
  defp using(ttl, nil),       do: {" USING TTL ?", [ttl]}
  defp using(nil, timestamp), do: {" USING TIMESTAMP ?", [timestamp]}
  defp using(ttl, timestamp), do: {" USING TTL ? AND TIMESTAMP ?", [ttl, timestamp]}

  defp prepend(str, prefix), do: prefix <> str

  defp ifelse(true,  a, _b), do: a
  defp ifelse(false, _a, b), do: b

  defp boolean([%{expr: expr} | exprs], sources, query) do
    Enum.reduce exprs, paren_expr(expr, sources, query), fn
      %BooleanExpr{expr: e, op: :and}, acc ->
        acc <> " AND " <> paren_expr(e, sources, query)
      %BooleanExpr{expr: e, op: :or}, acc ->
        acc <> " OR " <> paren_expr(e, sources, query)
    end
  end

  defp select_fields([], _sources, _query) do
    raise ArgumentError, "bad select clause"
  end

  defp select_fields(fields, sources, query) do
    Enum.map_join fields, ", ", fn
      {key, value} ->
        expr(value, sources, query) <> " AS " <> identifier(key)
      value ->
        expr(value, sources, query)
    end
  end

  defp identifier(name) when is_atom(name) do
    name |> Atom.to_string |> identifier
  end

  defp identifier(name) do
    if Regex.match?(@identifier, name) do
      name
    else
      raise ArgumentError, "bad identifier #{inspect name}"
    end
  end

  defp quote_name(name) when is_atom(name) do
    name |> Atom.to_string |> quote_name
  end

  defp quote_name(name) do
    if Regex.match?(@unquoted_name, name) do
      <<?", name::binary, ?">>
    else
      raise ArgumentError, "bad field name #{inspect name}"
    end
  end

  defp table_name(%{from: {table, _schema}, prefix: prefix}) do
    table_name(prefix, table)
  end

  defp table_name(nil, name),    do: table_name(name)
  defp table_name(prefix, name), do: table_name(prefix) <> "." <> table_name(name)

  defp table_name(name) when is_atom(name) do
    name |> Atom.to_string |> table_name
  end

  defp table_name(name) do
    if Regex.match?(@unquoted_name, name) do
      name
    else
      raise ArgumentError, "bad table name #{inspect name}"
    end
  end

  defp assemble(list) do
    list
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defp assemble_values(list) do
    {parts, values} =
      list
      |> Enum.map(fn
          nil            -> nil
          {part, values} -> {part, values}
          part           -> {part, []}
         end)
      |> Enum.reject(&is_nil/1)
      |> Enum.unzip

    IO.inspect({parts, values})

    {Enum.join(parts, " "), List.flatten(values)}
  end

  Enum.map @binary_operators_map, fn {op, term} ->
    defp call_type(unquote(op), 2), do: {:binary_operator, unquote(term)}
  end

  defp call_type(func, _arity), do: {:func, Atom.to_string(func)}

  defp paren_expr(expr, sources, query) do
    "(" <> expr(expr, sources, query) <> ")"
  end

  defp expr({:^, [], [_]}, _sources, _query), do: "?"

  defp expr({{:., _, [{:&, _, [_]}, field]}, _, []}, _sources, _query) when is_atom(field) do
    identifier(field)
  end

  defp expr({:&, _, [_idx, fields, _counter]}, _sources, _query) do
    Enum.map_join(fields, ", ", &identifier/1)
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    left = in_arg(left, sources, query)
    right = in_arg(right, sources, query)
    "#{left} IN #{right}"
  end

  defp in_arg(terms, sources, query) when is_list(terms) do
    "(" <> Enum.map_join(terms, ",", &expr(&1, sources, query)) <> ")"
  end

  defp in_arg(term, sources, query) do
    expr(term, sources, query)
  end

  defp expr({:fragment, _, [kw]}, _sources, _query) when is_list(kw) or tuple_size(kw) == 3 do
    raise ArgumentError, "Cassandra adapter does not support keyword or interpolated fragments for now!"
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map_join parts, "", fn
      {:raw, str}   -> str
      {:expr, expr} -> expr(expr, sources, query)
    end
  end

  defp expr({fun, _, args}, sources, query)
  when is_atom(fun) and is_list(args)
  do
    case call_type(fun, length(args)) do
      {:binary_operator, op} ->
        [left, right] = Enum.map(args, &binary_op_arg_expr(&1, sources, query))
        "#{left} #{op} #{right}"

      {:func, func} ->
        params = Enum.map_join(args, ", ", &expr(&1, sources, query))
        "#{func}(#{params})"
    end
  end

  defp expr(nil,   _sources, _query), do: "NULL"
  defp expr(true,  _sources, _query), do: "TRUE"
  defp expr(false, _sources, _query), do: "FALSE"

  defp expr(value, _sources, _query) when is_bitstring(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> uuid
      :error      -> "'#{escape_string(value)}'"
    end
  end

  defp expr(value, _sources, _query) when is_integer(value) or is_float(value) do
    "#{value}"
  end

  defp escape_string(value) when is_bitstring(value) do
    String.replace(value, "'", "''")
  end

  defp binary_op_arg_expr({op, _, [_, _]} = expr, sources, query)
  when op in @binary_operators do
    paren_expr(expr, sources, query)
  end

  defp binary_op_arg_expr(expr, sources, query) do
    expr(expr, sources, query)
  end
end
