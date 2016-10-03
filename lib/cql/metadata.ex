defmodule CQL.MetaData do
  require Bitwise
  import CQL.Decoder

  @global_spec    0x01
  @has_more_pages 0x02
  @no_metadata    0x04
  
  def unpack(buffer, meta) do
    Enum.reduce(meta, {%{}, buffer}, &unpack_one/2)
  end
  
  def unpack_one({_, {_, [when: false]}}, {map, buffer}) do
    {map, buffer}
  end
 
  def unpack_one({name, {func, [when: true]}}, {map, buffer}) do
    unpack_one({name, func}, {map, buffer})
  end
  
  def unpack_one({name, {func, [when: flag]}}, {map, buffer}) do
    unpack_one({name, {func, [when: matches(flag, map.flags)]}}, {map, buffer})
  end
  
  def unpack_one({name, {func, [unless: flag]}}, {map, buffer}) do
    unpack_one({name, {func, [when: !matches(flag, map.flags)]}}, {map, buffer})
  end
 
  def unpack_one({name, func}, {map, buffer}) do
    {value, buffer} = func.(buffer)
    {%{map | name => value}, buffer}
  end

  def decode(buffer, pk_indices \\ false) do
    {data, buffer} = unpack buffer, %{
      flags:         &int/1,
      columns_count: &int/1,
      pk_indices:    {&pk_indices/1, when: pk_indices},
      paging_state:  {&bytes/1, when: @has_more_pages},
    }
    
    columns_specs = fn buffer ->
      {global, buffer} = unpack buffer, %{
        flags: data.flags,
        spec: {&global_spec/1, when: @global_spec}
      }
      ntimes(data.columns_count, column_spec(global.spec), buffer)
    end
    
    {specs, buffer} = unpack buffer, %{
      columns_specs: {&columns_specs/1, unless: @no_metadata},
    }

    {Map.merge(data, specs), buffer}
  end

  def pk_indices(buffer) do
    {pk_count, buffer} = int(buffer)
    ntimes(pk_count, &short/1, buffer)
  end

  def global_spec(buffer) do
    {spec, buffer} = unpack buffer, %{
      keyspace: &string/1,
      table:    &string/1,
    }
  end

  def column_spec(nil) do
    fn buffer ->
      unpack buffer, %{
        keyspace: &string/1,
        table:    &string/1,
        name:     &string/1,
        type:     &option/1,
      }
    end
  end

  def column_spec(global) do
    fn buffer ->
      {spec, buffer} = unpack buffer, %{
        name:     &string/1,
        type:     &option/1,
      }
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
