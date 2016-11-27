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
    {cql, _} = apply(__MODULE__, operation, [query, options])
    cql
  end

  def all(query, options \\ []) do
    query = assemble [
      select(query),
      from(query),
      where(query),
      group_by(query),
      order_by(query),
      limit(query),
      lock(query),
    ]

    {query, options}
  end

  def delete_all(query, options) do
    table = table_name(query)
    query = case where(query) do
      [] ->
        "TRUNCATE #{table}"
      where ->
        assemble [
          "DELETE FROM #{table}",
          where,
          only_when(options[:if] == :exists, "IF EXISTS"),
          using(options[:ttl], options[:timestamp]),
        ]
    end

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, options}
  end

  def update_all(query, options) do
    where = where(query)
    if is_nil(where), do: raise ArgumentError, "Cassandra requires where caluse for update"

    query = assemble [
      "UPDATE",
      table_name(query),
      using(options[:ttl], options[:timestamp]),
      update_fields(query),
      where,
      only_when(options[:if] == :exists, "IF EXISTS"),
    ]

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, options}
  end

  defp update_fields(%{updates: updates} = query) do
    fields = for %{expr: expr} <- updates,
      {op, kw} <- expr,
      {key, value} <- kw
    do
      update_op(op, key, value, query)
    end
    ["SET", Enum.join(fields, ", ")]
  end

  defp update_op(op, key, value, query) do
    field = identifier(key)
    value = expr(value, query)
    case op do
      :set  -> assemble [field, "=", value]
      :inc  -> assemble [field, "=", field, "+", value]
      :push -> assemble [field, "=", field, "+", "[", value, "]"]
      :pull -> assemble [field, "=", field, "-", "[", value, "]"]
      other -> error!(query, "Unknown update operation #{inspect other} for Cassandra")
    end
  end

  def insert(prefix, source, fields, autogenerate, options) do
    autogenerate = Enum.map(autogenerate, fn {name, type} -> {name, column_type(type)} end)
    query = assemble [
      "INSERT INTO",
      table_name(prefix, source),
      values(autogenerate, fields),
      only_when(options[:if] == :not_exists, "IF NOT EXISTS"),
      using(options[:ttl], options[:timestamp]),
    ]

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, options}
  end

  def update(prefix, source, fields, filters, options) do
    # TODO: support IF conditions

    query = assemble [
      "UPDATE",
      table_name(prefix, source),
      using(options[:ttl], options[:timestamp]),
      set(fields),
      where(filters),
      only_when(options[:if] == :exists, "IF EXISTS"),
    ]

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, options}
  end

  def delete(prefix, source, filters, options) do
    # TODO: support IF conditions

    query = assemble [
      "DELETE FROM",
      table_name(prefix, source),
      using(options[:ttl], options[:timestamp]),
      where(filters),
      only_when(options[:if] == :exists, "IF EXISTS"),
    ]

    options = Keyword.drop(options, [:if, :ttl, :timestamp])

    {query, options}
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
      |> map

    if replication == "" do
      raise ":replication is nil in repository configuration"
    end

    durable_writes = Keyword.get(options, :durable_writes)

    with_cluse = case durable_writes do
      nil -> "WITH replication = #{replication}"
      _   -> "WITH replication = #{replication} AND durable_writes = #{durable_writes}"
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
    query = assemble [
      "BEGIN",
      only_when(options[:type] == :unlogged, "UNLOGGED"),
      only_when(options[:type] == :counter, "COUNTER"),
      "BATCH\n  ",
      using(options[:ttl], options[:timestamp]),
      Enum.join(queries, ";\n  "),
      "\nAPPLY BATCH",
    ]

    options = Keyword.drop(options, [:ttl, :timestamp])

    {query, options}
  end

  ### Helpers ###

  defp values(autogenerate, fields) do
    {auto_names, auto_values} =
      autogenerate
      |> Enum.map(fn {name, type} -> {name, autogenerate_value(type)} end)
      |> Enum.unzip

    {names, values} = Enum.unzip(fields)

    names = Enum.map_join(auto_names ++ names, ", ", &identifier/1)
    values = Enum.map_join(auto_values ++ values, ", ", &value/1)

    ["(#{names})", "VALUES", "(#{values})"]
  end

  defp autogenerate_value("timeuuid"), do: :now
  defp autogenerate_value("uuid"), do: :uuid

  defp value(:now),  do: "now()"
  defp value(:uuid), do: "uuid()"
  defp value(value), do: primitive(value)

  defp set(fields) do
    sets =
      fields
      |> Keyword.keys
      |> Enum.map(&"#{identifier(&1)} = ?")
      |> Enum.join(", ")

    ["SET", sets]
  end

  defp select(%{select: %{fields: fields}} = query) do
    ["SELECT", select_fields(fields, query)]
  end

  defp from(query) do
    ["FROM", table_name(query)]
  end

  defp where(filters) when is_list(filters) do
    conditions =
      filters
      |> Enum.map(fn {key, value} -> [identifier(key), "=", expr(value, nil)] end)
      |> Enum.intersperse("AND")

    ["WHERE", conditions]
  end

  defp where(%{wheres: []}), do: []
  defp where(%{wheres: wheres} = query) do
    ["WHERE", boolean(wheres, query)]
  end

  defp group_by(%{group_bys: []}), do: []
  defp group_by(%{group_bys: group_bys} = query) do
    group_by_clause =
      group_bys
      |> Enum.flat_map(fn %{expr: expr} -> expr end)
      |> Enum.map(&expr(&1, query))
      |> Enum.join(", ")

    ["GROUP BY", group_by_clause]
  end

  defp order_by(%{order_bys: []}), do: []
  defp order_by(%{order_bys: order_bys} = query) do
    ordering_clause =
      order_bys
      |> Enum.flat_map(fn %{expr: expr} -> expr end)
      |> Enum.map(&order_by_expr(&1, query))
      |> Enum.join(", ")

    ["ORDER BY", ordering_clause]
  end

  defp order_by_expr({dir, expr}, query) do
    assemble [
      expr(expr, query),
      only_when(dir == :desc, "DESC"),
    ]
  end

  defp limit(%{limit: nil}), do: []
  defp limit(%{limit: %{expr: expr}} = query) do
    ["LIMIT", expr(expr, query)]
  end

  defp lock(%{lock: nil}),                do: []
  defp lock(%{lock: "ALLOW FILTERING"}),  do: "ALLOW FILTERING"
  defp lock(query), do: support_error!(query, "locking")

  defp using(nil, nil),       do: []
  defp using(ttl, nil),       do: ["USING TTL", primitive(ttl)]
  defp using(nil, timestamp), do: ["USING TIMESTAMP", primitive(timestamp)]
  defp using(ttl, timestamp), do: ["USING TTL", primitive(ttl), "AND TIMESTAMP", primitive(timestamp)]

  defp only_when(true, a), do: a
  defp only_when(false, _), do: []
  defp only_when(x, a), do: only_when(!is_nil(x), a)

  defp boolean(exprs, query) do
    relations =
      Enum.map exprs, fn
        %BooleanExpr{expr: expr, op: :and} -> expr(expr, query)
        %BooleanExpr{op: :or} -> support_error!(query, "OR operator")
      end

    Enum.intersperse(relations, "AND")
  end

  defp select_fields([], query) do
    error!(query, "bad select clause")
  end

  defp select_fields(fields, query) do
    selectors =
      Enum.map fields, fn
        {key, value} ->
          assemble [expr(value, query), "AS", identifier(key)]
        value ->
          expr(value, query)
      end

    Enum.join(selectors, ", ")
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
    list |> List.flatten |> Enum.join(" ")
  end

  Enum.map @binary_operators_map, fn {op, term} ->
    defp call_type(unquote(op), 2), do: {:binary_operator, unquote(term)}
  end

  defp call_type(func, _arity), do: {:func, Atom.to_string(func)}

  defp expr({:^, [], [_]}, _query), do: "?"

  defp expr({{:., _, [{:&, _, [_]}, field]}, _, []}, _query) when is_atom(field) do
    identifier(field)
  end

  defp expr({:&, _, [_idx, fields, _counter]}, _query) do
    Enum.map_join(fields, ", ", &identifier/1)
  end

  defp expr({:in, _, [left, right]}, query) do
    assemble [expr(left, query), "IN", expr(right, query)]
  end

  defp expr({:is_nil, _, _}, query) do
    support_error!(query, "IS NULL relation")
  end

  defp expr({:not, _, _}, query) do
    support_error!(query, "NOT relation")
  end

  defp expr({:or, _, _}, query) do
    support_error!(query, "OR operator")
  end

  defp expr({:fragment, _, [kw]}, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "Cassandra adapter does not support keyword or fragments")
  end

  defp expr({:fragment, _, parts}, query) do
    Enum.map_join parts, "", fn
      {:raw, str}   -> str
      {:expr, expr} -> expr(expr, query)
    end
  end

  defp expr(list, query) when is_list(list) do
    items = Enum.map_join(list, ", ", &expr(&1, query))
    "(" <> items <> ")"
  end

  defp expr({fun, _, args}, query)
  when is_atom(fun) and is_list(args)
  do
    case call_type(fun, length(args)) do
      {:binary_operator, op} ->
        [left, right] = Enum.map(args, &binary_op_arg_expr(&1, query))
        "#{left} #{op} #{right}"

      {:func, func} ->
        "#{func}#{expr(args, query)}"
    end
  end

  defp expr(%Ecto.Query.Tagged{value: value}, query) do
    expr(value, query)
  end

  defp expr(value, _query)
  when is_nil(value) or
       value == true or
       value == false or
       is_binary(value) or
       is_integer(value) or
       is_float(value)
  do
    primitive(value)
  end

  defp primitive(nil),   do: "NULL"
  defp primitive(true),  do: "TRUE"
  defp primitive(false), do: "FALSE"
  defp primitive(:now),  do: "now()"
  defp primitive(:uuid), do: "uuid()"
  defp primitive(value) when is_binary(value) or is_atom(value), do: quote_string(value)
  defp primitive(value) when is_integer(value) or is_float(value), do: "#{value}"
  defp primitive(%DateTime{} = datetime), do: datetime |> DateTime.to_naive |> primitive
  defp primitive(%NaiveDateTime{microsecond: {mic, _}} = naive) do
    naive = %NaiveDateTime{naive | microsecond: {mic, 3}}
    primitive(NaiveDateTime.to_iso8601(naive) <> "+0000")
  end
  defp primitive(map) when is_map(map), do: map(map)
  defp primitive({_,_,_,_} = ip), do: ip |> Tuple.to_list |> Enum.join(".") |> primitive
  defp primitive({_,_,_,_,_,_} = ip), do: ip |> Tuple.to_list |> Enum.join(":") |> primitive

  defp map(map) do
    "{" <> Enum.map_join(map, ", ", fn {key, value} -> primitive(key) <> " : " <> primitive(value) end) <> "}"
  end

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

  defp binary_op_arg_expr({op, _, [_, _]} = expr, query)
  when op in @binary_operators do
    expr(expr, query)
  end

  defp binary_op_arg_expr(expr, query) do
    expr(expr, query)
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

    "(" <> fields <> ")"
  end

  defp table_options(%Table{options: nil, comment: nil}),
    do: []
  defp table_options(%Table{options: nil, comment: comment}),
    do: "WITH comment=#{quote_string(comment)}"
  defp table_options(%Table{options: options, comment: nil}),
    do: options
  defp table_options(%Table{options: options, comment: comment}),
    do: "#{options} AND comment=#{quote_string(comment)}"

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

  defp column_type({:map, {ktype, vtype}}),
    do: "MAP<#{column_type(ktype)}, #{column_type(vtype)}>"
  defp column_type({:map, type}),
    do: "MAP<text, #{column_type(type)}>"
  defp column_type(:map),
    do: "MAP<text, text>"
  defp column_type({:array, type}),
    do: "LIST<#{column_type(type)}>"
  defp column_type({:set, type}),
    do: "SET<#{column_type(type)}>"
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
        []
      end
    end
  end

  defp column_changes([]), do: []
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
    ["ADD", changes]
  end

  defp column_changes(:remove, columns) do
    changes = Enum.map_join columns, " ", fn
      {:remove, name, _, _} -> identifier(name)
    end
    ["DROP", changes]
  end

  defp column_changes(:modify, [{:modify, name, type, _options}]) do
    [identifier(name), "TYPE", column_type(type)]
  end

  defp column_changes(:modify, _columns) do
    migration_support_error!("altering multiple columns")
  end
end
