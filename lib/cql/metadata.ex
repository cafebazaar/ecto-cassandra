defmodule CQL.MetaData do
  import CQL.DataTypes.Decoder

  require Bitwise

  @global_spec    0x01
  @has_more_pages 0x02
  @no_metadata    0x04

  def decode(buffer, pk_indices \\ false) do
    {data, buffer} = unpack buffer,
      flags:         :int,
      columns_count: :int,
      pk_indices:    {&pk_indices/1, when: pk_indices},
      paging_state:  {:bytes, when: @has_more_pages}

    {specs, buffer} = unpack buffer,
      columns_specs: {columns_specs(data), unless: matches(@no_metadata, data.flags)}

    {Map.merge(data, specs), buffer}
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

  def columns_specs(data) do
    fn buffer ->
      {global, buffer} = unpack buffer,
        spec: {&global_spec/1, when: matches(@global_spec, data.flags)}
      global_spec = Map.get(global, :spec)
      ntimes(data.columns_count, column_spec(global_spec), buffer)
    end
  end

  def column_spec(nil) do
    fn buffer ->
      unpack buffer,
        keyspace: :string,
        table:    :string,
        name:     :string,
        type:     &option/1
    end
  end

  def column_spec(global) do
    fn buffer ->
      {spec, buffer} = unpack buffer,
        name:     :string,
        type:     &option/1
      {Map.merge(global, spec), buffer}
    end
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

    {{id, value}, buffer}
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
