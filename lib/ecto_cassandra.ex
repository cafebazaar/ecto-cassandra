defmodule EctoCassandra do
  @moduledoc false

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
    {query, values, _} = apply(__MODULE__, operation, [query, options])
    {query, values}
  end

  def all(%{sources: sources} = query, options \\ []) do
    {query, values} = assemble_values([
      select(query, sources),
      from(query, sources),
      where(query, sources),
      group_by(query, sources),
      order_by(query, sources),
      limit(query, sources),
      lock(query),
    ])

    {query, values, options}
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
          only_when(options[:if] == :exists, "IF EXISTS"),
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
      only_when(options[:if] == :exists, "IF EXISTS"),
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
    assemble_values ["SET", join_values(fields, ", ")]
  end

  defp update_op(op, key, value, sources, query) do
    field = identifier(key)
    value = expr(value, sources, query)
    case op do
      :set  -> assemble_values [field, "=", value]
      :inc  -> assemble_values [field, "=", field, "+", value]
      :push -> assemble_values [field, "=", field, "+", "[", value, "]"]
      :pull -> assemble_values [field, "=", field, "-", "[", value, "]"]
      other -> error!(query, "Unknown update operation #{inspect other} for Cassandra")
    end
  end

  def insert(prefix, source, fields, autogenerate, options) do
    autogenerate = Enum.map(autogenerate, fn {name, type} -> {name, column_type(type)} end)
    {query, values} = assemble_values([
      "INSERT INTO",
      table_name(prefix, source),
      values(autogenerate, fields),
      only_when(options[:if] == :not_exists, "IF NOT EXISTS"),
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
      only_when(options[:if] == :exists, "IF EXISTS"),
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
      only_when(options[:if] == :exists, "IF EXISTS"),
    ])

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, values, options}
  end

  def ddl({command, %Table{} = table, columns})
  when command in [:create, :create_if_not_exists]
  do
    assemble [
      "CREATE TABLE",
      only_when(command == :create_if_not_exists, "IF NOT EXISTS"),
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
      only_when(command == :drop_if_exists, "IF EXISTS"),
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
      only_when(index.using, "CUSTOM"),
      "INDEX",
      only_when(command == :create_if_not_exists, "IF NOT EXISTS"),
      index_name(index.prefix, index.name),
      "ON",
      table_name(index.prefix, index.table),
      index_identifiers(index),
      only_when(index.using, "USING #{index.using}"),
    ]
  end

  def ddl({command, %Index{} = index})
  when command in [:drop, :drop_if_exists]
  do
    assemble [
      "DROP INDEX",
      only_when(command == :drop_if_exists, "IF EXISTS"),
      index_name(index.prefix, index.name),
    ]
  end

  def create_keyspace(options) do
    keyspace = Keyword.fetch!(options, :keyspace) || raise ":keyspace is nil in repository configuration"

    replication =
      options
      |> Keyword.get(:replication, [])
      |> Enum.map_join(", ", fn {key, value} -> "#{quote_string(key)}: #{primitive(value)}" end)

    if replication == "" do
      raise ":replication is nil in repository configuration"
    end

    durable_writes = Keyword.get(options, :durable_writes)

    with_cluse = case durable_writes do
      nil -> "WITH replication = {#{replication}}"
      _   -> "WITH replication = {#{replication}} AND durable_writes = #{durable_writes}"
    end

    assemble [
      "CREATE KEYSPACE",
      only_when(options[:if_not_exists], "IF NOT EXISTS"),
      keyspace,
      with_cluse,
    ]
  end

  def drop_keyspace(options) do
    keyspace = Keyword.fetch!(options, :keyspace) || raise ":keyspace is nil in repository configuration"

    assemble [
      "DROP KEYSPACE",
      only_when(options[:if_exists], "IF EXISTS"),
      keyspace,
    ]
  end

  def batch(queries, options) do
    {query, values} = assemble_values [
      "BEGIN",
      only_when(options[:type] == :unlogged, "UNLOGGED"),
      only_when(options[:type] == :counter, "COUNTER"),
      "BATCH",
      using(options[:ttl], options[:timestamp]),
      Enum.join(queries, "; "),
      "APPLY BATCH",
    ]

    options = Keyword.drop(options, [:ttl, :timestamp])

    {query, values, options}
  end

  ### Helpers ###

  defp values(autogenerate, fields) do
    {auto_names, auto_values} =
      autogenerate
      |> Enum.map(fn {name, type} -> {name, autogenerate_value(type)} end)
      |> Enum.unzip

    {names, values} = Enum.unzip(fields)
    names = Enum.map_join(auto_names ++ names, ", ", &identifier/1)
    [marks, values] =
      (auto_values ++ values)
      |> Enum.map(&value/1)
      |> Enum.unzip
      |> Tuple.to_list
      |> Enum.map(&compact/1)

    {"(#{names}) VALUES (#{Enum.join(marks, ", ")})", values}
  end

  defp autogenerate_value("timeuuid"), do: :now
  defp autogenerate_value("uuid"), do: :uuid

  defp compact(list), do: Enum.reject(list, &is_nil/1)

  defp value(:now), do: {"now()", nil}
  defp value(:uuid), do: {"uuid()", nil}
  defp value(value), do: {"?", value}

  defp set(fields) do
    {names, values} = Enum.unzip(fields)
    sets = Enum.map_join(names, ", ", &"#{identifier(&1)} = ?")
    {"SET #{sets}", values}
  end

  defp select(%{select: %{fields: fields}} = query, sources) do
    assemble_values ["SELECT", select_fields(fields, sources, query)]
  end

  defp from(query, _sources) do
    assemble_values ["FROM", table_name(query)]
  end

  defp where(filters) when is_list(filters) do
    {fields, values} = Enum.unzip(filters)
    conditions = Enum.map_join(fields, " AND ", &"#{identifier(&1)} = ?")
    {"WHERE #{conditions}", values}
  end

  defp where(%{wheres: []}, _), do: nil
  defp where(%{wheres: wheres} = query, sources) do
    assemble_values [
      "WHERE",
      boolean(wheres, sources, query),
    ]
  end

  defp group_by(%{group_bys: []}, _), do: nil
  defp group_by(%{group_bys: group_bys} = query, sources) do
    group_by_clause =
      group_bys
      |> Enum.flat_map(fn %{expr: expr} -> expr end)
      |> Enum.map(&expr(&1, sources, query))
      |> join_values(", ")

    assemble_values ["GROUP BY", group_by_clause]
  end

  defp order_by(%{order_bys: []}, _), do: nil
  defp order_by(%{order_bys: order_bys} = query, sources) do
    ordering_clause =
      order_bys
      |> Enum.flat_map(fn %{expr: expr} -> expr end)
      |> Enum.map(&order_by_expr(&1, sources, query))
      |> join_values(", ")

    assemble_values ["ORDER BY", ordering_clause]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    assemble_values [
      expr(expr, sources, query),
      only_when(dir == :desc, "DESC"),
    ]
  end

  defp limit(%{limit: nil}, _sources), do: nil
  defp limit(%{limit: %{expr: expr}} = query, sources) do
    assemble_values ["LIMIT", expr(expr, sources, query)]
  end

  defp lock(%{lock: nil}), do: nil
  defp lock(%{lock: "ALLOW FILTERING"}), do: "ALLOW FILTERING"
  defp lock(query), do: support_error!(query, "locking")

  defp using(nil, nil),       do: nil
  defp using(ttl, nil),       do: {" USING TTL ?", [ttl]}
  defp using(nil, timestamp), do: {" USING TIMESTAMP ?", [timestamp]}
  defp using(ttl, timestamp), do: {" USING TTL ? AND TIMESTAMP ?", [ttl, timestamp]}

  defp only_when(true, a), do: a
  defp only_when(false, _), do: nil
  defp only_when(x, a), do: only_when(!is_nil(x), a)

  defp boolean(exprs, sources, query) do
    relations =
      Enum.map exprs, fn
        %BooleanExpr{expr: expr, op: :and} -> expr(expr, sources, query)
        %BooleanExpr{op: :or} -> support_error!(query, "OR operator")
      end

    join_values(relations, " AND ")
  end

  defp select_fields([], _sources, query) do
    error!(query, "bad select clause")
  end

  defp select_fields(fields, sources, query) do
    selectors =
      Enum.map fields, fn
        {key, value} ->
          assemble_values [expr(value, sources, query), "AS", identifier(key)]
        value ->
          expr(value, sources, query)
      end

    join_values(selectors, ", ")
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
    list |> compact |> Enum.join(" ")
  end

  defp assemble_values(list) when is_list(list) do
    join_values(list, " ")
  end

  defp join_values(list, joiner \\ "") when is_list(list) do
    {parts, values} =
      list
      |> Enum.map(fn
          nil            -> nil
          {part, values} -> {part, values}
          part           -> {part, []}
         end)
      |> compact
      |> Enum.unzip

    {Enum.join(parts, joiner), Enum.concat(values)}
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
    items =
      right
      |> Enum.map(&primitive(&1))
      |> join_values(", ")

    join_values [expr(left, sources, query), " IN (", items, ")"]
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
    parts =
      Enum.map parts, fn
        {:raw, str}   -> str
        {:expr, expr} -> expr(expr, sources, query)
      end

    join_values parts
  end

  defp expr(list, sources, query) when is_list(list) do
    items =
      list
      |> Enum.map(&expr(&1, sources, query))
      |> join_values(", ")

    assemble_values ["(", items, ")"]
  end

  defp expr({fun, _, args}, sources, query)
  when is_atom(fun) and is_list(args)
  do
    case call_type(fun, length(args)) do
      {:binary_operator, op} ->
        [left, right] = Enum.map(args, &binary_op_arg_expr(&1, sources, query))
        assemble_values [left, op, right]

      {:func, func} ->
        params =
          args
          |> Enum.map(&expr(&1, sources, query))
          |> join_values(", ")

        join_values [func, "(", params, ")"]
    end
  end

  defp expr(%Ecto.Query.Tagged{value: value}, sources, query) do
    expr(value, sources, query)
  end

  defp expr(value, _sources, _query)
  when is_nil(value) or
       value == true or
       value == false or
       is_binary(value) or
       is_integer(value) or
       is_float(value)
  do
    {"?", [value]}
  end

  defp primitive(nil), do: "NULL"
  defp primitive(true), do: "TRUE"
  defp primitive(false), do: "FALSE"
  defp primitive(value) when is_bitstring(value), do: quote_string(value)
  defp primitive(value) when is_integer(value) or is_float(value), do: "#{value}"

  defp quote_string(value) when is_atom(value) do
    value |> Atom.to_string |> quote_string
  end

  defp quote_string(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> uuid
      :error      -> "'#{escape_string(value)}'"
    end
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
    "WITH comment=#{quote_string(comment)}"
  end

  defp table_options(%Table{options: options, comment: nil}) do
    options
  end

  defp table_options(%Table{options: options, comment: comment}) do
    "#{options} AND comment=#{quote_string(comment)}"
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
    Keyword.has_key?(options, :partition_key) or Keyword.has_key?(options, :primary_key)
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
    "MAP<#{column_type(ktype)}, #{column_type(vtype)}>"
  end

  defp column_type({:map, type}) do
    "MAP<text, #{column_type(type)}>"
  end

  defp column_type(:map) do
    "MAP<text, text>"
  end

  defp column_type({:array, type}) do
    "LIST<#{column_type(type)}>"
  end

  defp column_type({:set, type}) do
    "SET<#{column_type(type)}>"
  end

  defp column_type(:id),             do: "uuid"
  defp column_type(:binary_id),      do: "timeuuid"
  defp column_type(:uuid),           do: "uuid"
  defp column_type(:timeuuid),       do: "timeuuid"
  defp column_type(:integer),        do: "int"
  defp column_type(:string),         do: "text"
  defp column_type(:binary),         do: "blob"
  defp column_type(:utc_datetime),   do: "timestamp"
  defp column_type(:naive_datetime), do: "timestamp"
  defp column_type(:float),          do: "double"
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
