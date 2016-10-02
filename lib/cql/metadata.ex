defmodule CQL.MetaData do
  require Bitwise
  import CQL.Decoder

  @global_spec    0x01
  @has_more_pages 0x02
  @no_metadata    0x04

  def decode(binary, pk_indices \\ false) do
    {flags,         x} = int(binary)
    {columns_count, x} = int(x)
    {pk_indices,    x} = run_when(&pk_indices/1, x, pk_indices)

    cols_specs = fn binary ->
      {global_spec, x} = run_when_matches(&global_spec/1, binary, @global_spec, flags)
      ntimes(columns_count, column_spec(global_spec), x)
    end

    {paging_state,  x} = run_when_matches(&bytes/1, x, @has_more_pages, flags)
    {columns_specs, x} = run_when(cols_specs, x, !matches(@no_metadata, flags))

    metadata = %{
      flags: flags,
      columns_count: columns_count,
      pk_indices: pk_indices,
      paging_state: paging_state,
      columns_specs: columns_specs,
    }

    {metadata, x}
  end

  def pk_indices(binary) do
    {pk_count, x} = int(binary)
    ntimes(pk_count, &short/1, x)
  end

  def global_spec(binary) do
    {keyspace, x} = string(binary)
    {table,   x} = string(x)

    {{keyspace, table}, x}
  end

  def column_spec(nil) do
    fn binary ->
      {keyspace, x} = string(binary)
      {table,    x} = string(x)
      {name,     x} = string(x)
      {type,     x} = option(x)

      {{keyspace, table, name, type}, x}
    end
  end

  def column_spec({keyspace, table}) do
    fn binary ->
      {name, x} = string(binary)
      {type, x} = option(x)

      {{keyspace, table, name, type}, x}
    end
  end

  def option(binary) do
    {id,    x} = short(binary)
    {value, x} = case id do
      0 -> string(x)
      0x20 -> option(x)
      0x21 -> option_pair(x)
      0x22 -> option(x)
      #TODO: complete me
      _ -> {nil, x}
    end

    {{id, value}, x}
  end

  def option_pair(binary) do
    {option1, x} = option(binary)
    {option2, x} = option(x)

    {{option1, option2}, x}
  end
end
