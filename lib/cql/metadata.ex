defmodule CQL.MetaData do
  import CQL.DataTypes.Decoder

  require Bitwise

  @flags %{
    :global_spec    => 0x01,
    :has_more_pages => 0x02,
    :no_metadata    => 0x04,
  }

  def decode(buffer, pk_indices \\ false) do
    {meta, buffer} = unpack buffer,
      flags:         :int,
      columns_count: :int,
      pk_indices:    {&pk_indices/1, when: pk_indices},
      paging_state:  {:bytes, when: @flags.has_more_pages}

    no_meta? = flag?(@flags.no_metadata, meta.flags)
    global?  = flag?(@flags.global_spec, meta.flags)

    case {no_meta?, global?} do
      {true, _} ->
        {meta, buffer}
      {false, true} ->
        {global_spec, buffer}  = global_spec(buffer)
        {column_types, buffer} = ntimes(meta.columns_count, &column_type/1, buffer)
        {Map.merge(meta, %{column_types: column_types, global_spec: global_spec}), buffer}
      {false, false} ->
        {specs, buffer} = column_specs(meta.columns_count, buffer)
        {Map.merge(meta, specs), buffer}
    end
  end

  def pk_indices(buffer) do
    {pk_count, buffer} = int(buffer)
    ntimes(pk_count, :short, buffer)
  end

  def global_spec(buffer) do
    unpack buffer,
      keyspace: :string,
      table:    :string
  end

  def column_specs(n, buffer) do
    {specs, buffer} = ntimes(n, &column_spec/1, buffer)
    {tables, types} = Enum.unzip(specs)
    {%{column_types: types, column_specs: tables}, buffer}
  end

  def column_spec(buffer) do
    {keyspace, buffer} = string(buffer)
    {table,    buffer} = string(buffer)
    {name,     buffer} = string(buffer)
    {type,     buffer} = option(buffer)
    {{{keyspace, table}, {name, type}}, buffer}
  end

  def column_type(buffer) do
    {name, buffer} = string(buffer)
    {type, buffer} = option(buffer)
    {{name, type}, buffer}
  end

  def option(buffer) do
    {id,    buffer} = short(buffer)
    {value, buffer} = case id do
      0x00 -> string(buffer)
      0x20 -> option(buffer)
      0x21 -> options_pair(buffer)
      0x22 -> option(buffer)
      0x30 -> {nil, buffer} # TODO: UDT
      0x31 -> options(buffer)
      _    -> {nil, buffer}
    end

    {CQL.DataTypes.kind({id, value}), buffer}
  end

  def options(buffer) do
    {n, buffer} = short(buffer)
    ntimes(n, &option/1, buffer)
  end

  def options_pair(buffer) do
    {option1, buffer} = option(buffer)
    {option2, buffer} = option(buffer)

    {{option1, option2}, buffer}
  end
end
