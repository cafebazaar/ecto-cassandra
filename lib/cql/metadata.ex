defmodule CQL.MetaData do
  require Bitwise
  import CQL.Decoder

  @global_spec    0x01
  @has_more_pages 0x02
  @no_metadata    0x04

  def decode(buffer, pk_indices \\ false) do
    {data, buffer} = unpack buffer,
      flags:         &int/1,
      columns_count: &int/1,
      pk_indices:    {&pk_indices/1, when: pk_indices},
      paging_state:  {&bytes/1, when: @has_more_pages}

    {specs, buffer} = unpack buffer,
      columns_specs: {columns_specs(data), unless: matches(@no_metadata, data.flags)}

    {Map.merge(data, specs), buffer}
  end

  def pk_indices(buffer) do
    {pk_count, buffer} = int(buffer)
    ntimes(pk_count, &short/1, buffer)
  end

  def global_spec(buffer) do
    unpack buffer,
      keyspace: &string/1,
      table:    &string/1
  end

  def columns_specs(data) do
    fn buffer ->
      {global, buffer} = unpack buffer,
        spec: {&global_spec/1, when: matches(@global_spec, data.flags)}
      ntimes(data.columns_count, column_spec(global.spec), buffer)
    end
  end

  def column_spec(nil) do
    fn buffer ->
      unpack buffer,
        keyspace: &string/1,
        table:    &string/1,
        name:     &string/1,
        type:     &option/1
    end
  end

  def column_spec(global) do
    fn buffer ->
      {spec, buffer} = unpack buffer,
        name:     &string/1,
        type:     &option/1
      {Map.merge(global, spec), buffer}
    end
  end

  def option(buffer) do
    {id,    buffer} = short(buffer)
    {value, buffer} = case id do
      0x00 -> string(buffer)
      0x20 -> option(buffer)
      0x21 -> option_pair(buffer)
      0x22 -> option(buffer)
      #TODO: complete me
      _    -> {nil, buffer}
    end

    {{id, value}, buffer}
  end

  def option_pair(buffer) do
    {option1, buffer} = option(buffer)
    {option2, buffer} = option(buffer)

    {{option1, option2}, buffer}
  end
end
