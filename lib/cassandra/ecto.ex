defmodule Cassandra.Ecto do

  alias Ecto.Query.BooleanExpr

  alias Ecto.Migration.{Table, Index, Reference}

  @index_name ~r/^[a-zA-Z_0-9]+$/
  @identifier ~r/^[a-zA-Z][a-zA-Z0-9_]*$/
  @unquoted_name ~r/^[a-zA-Z_0-9]{1,48}$/
  @binary_operators_map %{
    :==  => "=",
    :<   => "<",
    :>   => ">",
    :<=  => "<=",
    :>=  => ">=",
    :!=  => "!=",
    :and => "AND",
  }
  @binary_operators Map.keys(@binary_operators_map)

  ### API ###

  def to_cql(query, operation, options \\ []) do
    {query, _, _} = apply(__MODULE__, operation, [query, options])
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
      lock(query),
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
          where,
          ifelse(options[:if] == :exists, "IF EXISTS", nil),
          using(options[:ttl], options[:timestamp]),
        ])
    end

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  def update_all(%{sources: sources} = query, options) do
    {query, values} = assemble_values([
      "UPDATE",
      table_name(query),
      using(options[:ttl], options[:timestamp]),
      update_fields(query, sources),
      where(query, sources),
      ifelse(options[:if] == :exists, "IF EXISTS", nil),
    ])

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  defp update_fields(%{updates: updates} = query, sources) do
    fields = for %{expr: expr} <- updates,
        {op, kw} <- expr,
        {key, value} <- kw
    do
      update_op(op, key, value, sources, query)
    end
    "SET #{Enum.join(fields, ", ")}"
  end

  defp update_op(op, key, value, sources, query) do
    field = identifier(key)
    value = expr(value, sources, query)
    case op do
      :set  -> "#{field} = #{value}"
      :inc  -> "#{field} = #{field} + #{value}"
      :push -> "#{field} = #{field} + [#{value}]"
      :pull -> "#{field} = #{field} - [#{value}]"
      other -> error!(query, "Unknown update operation #{inspect other} for Cassandra")
    end
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

  def update(prefix, source, fields, filters, options) do
    # TODO: support IF conditions

    {query, values} = assemble_values([
      "UPDATE",
      table_name(prefix, source),
      using(options[:ttl], options[:timestamp]),
      set(fields),
      where(filters),
      ifelse(options[:if] == :exists, "IF EXISTS", nil),
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

  def ddl({command, %Table{} = table, columns})
  when command in [:create, :create_if_not_exists]
  do
    assemble [
      "CREATE TABLE",
      ifelse(command == :create_if_not_exists, "IF NOT EXISTS", nil),
      table_name(table.prefix, table.name),
      column_definitions(columns),
      table_options(table),
    ]
  end

  def ddl({command, %Table{} = table})
  when command in [:drop, :drop_if_exists]
  do
    assemble [
      "DROP TABLE",
      ifelse(command == :drop_if_exists, "IF EXISTS", nil),
      table_name(table.prefix, table.name),
    ]
  end

  def ddl({:alter, %Table{} = table, columns}) do
    assemble [
      "ALTER TABLE",
      table_name(table.prefix, table.name),
      column_changes(columns),
      table_options(table),
    ]
  end

  def ddl({command, %Index{} = index})
  when command in [:create, :create_if_not_exists]
  do
    assemble [
      "CREATE",
      ifelse(index.using, "CUSTOM", nil),
      "INDEX",
      ifelse(command == :create_if_not_exists, "IF NOT EXISTS", nil),
      index_name(index.prefix, index.name),
      "ON",
      table_name(index.prefix, index.table),
      index_identifiers(index),
      ifelse(index.using, "USING #{index.using}", nil),
    ]
  end

  def ddl({command, %Index{} = index})
  when command in [:drop, :drop_if_exists]
  do
    assemble [
      "DROP INDEX",
      ifelse(command == :drop_if_exists, "IF EXISTS", nil),
      index_name(index.prefix, index.name),
    ]
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

  defp set(fields) do
    {names, values} = Enum.unzip(fields)
    sets = Enum.map_join(names, ", ", &"#{identifier(&1)} = ?")
    {"SET #{sets}", values}
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

  defp lock(%{lock: nil}), do: nil
  defp lock(%{lock: "ALLOW FILTERING"}), do: "ALLOW FILTERING"
  defp lock(query), do: support_error!(query, "locking")

  defp using(nil, nil),       do: nil
  defp using(ttl, nil),       do: {" USING TTL ?", [ttl]}
  defp using(nil, timestamp), do: {" USING TIMESTAMP ?", [timestamp]}
  defp using(ttl, timestamp), do: {" USING TTL ? AND TIMESTAMP ?", [ttl, timestamp]}

  defp prepend(str, prefix), do: prefix <> str

  defp ifelse(true,  a, _b), do: a
  defp ifelse(false, _a, b), do: b
  defp ifelse(x, a, b), do: ifelse(is_nil(x), b, a)

  defp boolean(exprs, sources, query) do
    Enum.map_join exprs, " AND ", fn
      %BooleanExpr{expr: expr, op: :and} -> expr(expr, sources, query)
      %BooleanExpr{op: :or} -> support_error!(query, "OR operator")
    end
  end

  defp select_fields([], _sources, query) do
    error!(query, "bad select clause")
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

  defp index_name(nil, name),    do: index_name(name)
  defp index_name(prefix, name), do: table_name(prefix) <> "." <> index_name(name)

  defp index_name(name) when is_atom(name) do
    name |> Atom.to_string |> index_name
  end

  defp index_name(name) do
    if Regex.match?(@index_name, name) do
      name
    else
      raise ArgumentError, "bad index name #{inspect name}"
    end
  end

  defp table_name(%{from: {table, _schema}, prefix: prefix}) do
    table_name(prefix, table)
  end

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

  defp table_name(nil, name),    do: table_name(name)
  defp table_name(prefix, name), do: table_name(prefix) <> "." <> table_name(name)

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

    {Enum.join(parts, " "), List.flatten(values)}
  end

  Enum.map @binary_operators_map, fn {op, term} ->
    defp call_type(unquote(op), 2), do: {:binary_operator, unquote(term)}
  end

  defp call_type(func, _arity), do: {:func, Atom.to_string(func)}

  defp expr({:^, [], [_]}, _sources, _query), do: "?"

  defp expr({{:., _, [{:&, _, [_]}, field]}, _, []}, _sources, _query) when is_atom(field) do
    identifier(field)
  end

  defp expr({:&, _, [_idx, fields, _counter]}, _sources, _query) do
    Enum.map_join(fields, ", ", &identifier/1)
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    left = in_arg(left, sources, query)
    right = in_arg(right, sources, query)
    "#{left} IN #{right}"
  end

  defp expr({:in, _, [_, {:^, _, _}]}, _sources, query) do
    support_error!(query, "NOT IN relation")
  end

  defp expr({:is_nil, _, _}, _sources, query) do
    support_error!(query, "IS NULL relation")
  end

  defp expr({:not, _, _}, _sources, query) do
    support_error!(query, "NOT relation")
  end

  defp expr({:or, _, _}, _sources, query) do
    support_error!(query, "OR operator")
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "Cassandra adapter does not support keyword or interpolated fragments")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map_join parts, "", fn
      {:raw, str}   -> str
      {:expr, expr} -> expr(expr, sources, query)
    end
  end

  defp expr(list, sources, query) when is_list(list) do
    "(" <> Enum.map_join(list, ", ", &expr(&1, sources, query)) <> ")"
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

  defp in_arg(terms, sources, query) when is_list(terms) do
    "(" <> Enum.map_join(terms, ",", &expr(&1, sources, query)) <> ")"
  end

  defp in_arg(term, sources, query) do
    expr(term, sources, query)
  end

  defp escape_string(value) when is_bitstring(value) do
    String.replace(value, "'", "''")
  end

  defp binary_op_arg_expr({op, _, [_, _]} = expr, sources, query)
  when op in @binary_operators do
    expr(expr, sources, query)
  end

  defp binary_op_arg_expr(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp support_error!(query, message) do
    raise Ecto.QueryError, query: query, message: "Cassandra does not support #{message}"
  end

  defp migration_support_error!(message) do
    raise Ecto.MigrationError, message: "Cassandra does not support #{message}"
  end

  defp index_identifiers(%Index{columns: columns}) do
    fields = Enum.map_join columns, ", ", fn
      literal when is_binary(literal) -> literal
      name -> identifier(name)
    end

    "(#{fields})"
  end

  defp table_options(%Table{options: nil, comment: nil}), do: nil

  defp table_options(%Table{options: nil, comment: comment}) do
    "WITH comment=#{expr(comment, nil, nil)}"
  end

  defp table_options(%Table{options: options, comment: nil}) do
    options
  end

  defp table_options(%Table{options: options, comment: comment}) do
    "#{options} AND comment=#{expr(comment, nil, nil)}"
  end

  defp primary_key_definition(columns) do
    partition_key =
      columns
      |> Enum.filter(&partition_key?/1)
      |> Enum.map(fn {_, name, _, _} -> identifier(name) end)

    if match?([], partition_key) do
      raise Ecto.MigrationError, message: "Cassandra requires PRIMARY KEY"
    end

    partition_key = case partition_key do
      [partition_key] -> "#{partition_key}"
      partition_keys  -> "(#{Enum.join(partition_keys, ", ")})"
    end

    columns
    |> Enum.filter(&clustering_column?/1)
    |> Enum.map_join(", ", fn {_, name, _, _} -> identifier(name) end)
    |> case do
      "" -> "PRIMARY KEY (#{partition_key})"
      cc -> "PRIMARY KEY (#{partition_key}, #{cc})"
    end
  end

  defp partition_key?({_, _, _, options}) do
    Keyword.has_key?(options, :partition_key)
  end

  defp clustering_column?({_, _, _, options}) do
    Keyword.has_key?(options, :clustering_column)
  end

  defp column_definitions(columns) do
    defs = Enum.map_join columns, ", ", &column_definition/1
    pk   = primary_key_definition(columns)
    "(#{defs}, #{pk})"
  end

  defp column_definition({_, _, %Reference{}, _}) do
    migration_support_error! "references"
  end

  defp column_definition({:add, name, type, options}) do
    assemble [
      identifier(name),
      column_type(type),
      column_options(options),
    ]
  end

  defp column_type({:map, {ktype, vtype}}) do
    "MAP<#{column_type(ktype)},#{column_type(vtype)}>"
  end

  defp column_type({:array, type}) do
    "LIST<#{column_type(type)}>"
  end

  defp column_type({:set, type}) do
    "SET<#{column_type(type)}>"
  end

  defp column_type(:id),             do: "uuid"
  defp column_type(:binary_id),      do: "timeuuid"
  defp column_type(:string),         do: "text"
  defp column_type(:binary),         do: "blob"
  defp column_type(:utc_datetime),   do: "timestamp"
  defp column_type(:naive_datetime), do: "timestamp"
  defp column_type(other),           do: Atom.to_string(other)

  defp column_options(options) do
    if Keyword.has_key?(options, :static) do
      "STATIC"
    else
      if Keyword.has_key?(options, :comment) do
        migration_support_error!("columns comment")
      else
        nil
      end
    end
  end

  defp column_changes([]), do: nil
  defp column_changes([{change, _, _, _} | _] = columns) do
    if Enum.all?(columns, fn {c, _, _, _} -> c == change end) do
      column_changes(change, columns)
    else
      raise migration_support_error!("ALTER TABLE with different change types")
    end
  end

  defp column_changes(:add, columns) do
    changes = Enum.map_join columns, ", ", fn
      {:add, name, type, _} -> "#{identifier(name)} #{column_type(type)}"
    end

    "ADD #{changes}"
  end

  defp column_changes(:remove, columns) do
    changes = Enum.map_join columns, " ", fn
      {:remove, name, _, _} -> identifier(name)
    end

    "DROP #{changes}"
  end

  defp column_changes(:modify, [{:modify, name, type, _options}]) do
    "#{identifier(name)} TYPE #{column_type(type)}"
  end

  defp column_changes(:modify, _columns) do
    migration_support_error!("altering multiple columns")
  end
end
