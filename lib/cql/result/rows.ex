defmodule CQL.Result.Rows do
  import CQL.Decoder

  defstruct [
    :metadata,
    :rows_count,
    :rows_content,
  ]

  @ascii     0x01
  @bigint    0x02
  @blob      0x03
  @boolean   0x04
  @counter   0x05
  @decimal   0x06
  @double    0x07
  @float     0x08
  @int       0x09
  @timestamp 0x0B
  @uuid      0x0C
  @varchar   0x0D
  @varint    0x0E
  @timeuuid  0x0F
  @inet      0x10
  @list      0x20
  @map       0x21
  @set       0x22
  @udt       0x30
  @tuple     0x31


  def decode(binary) do
    {metadata,      x} = CQL.MetaData.decode(binary)
    {rows_count,    x} = int(x)
    {rows_content, ""} = ntimes(rows_count, row_content(metadata), x)

    %__MODULE__{
      metadata: metadata,
      rows_count: rows_count,
      rows_content: rows_content,
    }
  end

  def row_content(metadata) do
    fn binary ->
      {row, rest} = ntimes(metadata.columns_count, &bytes/1, binary)
      {parse(row, metadata.columns_specs), rest}
    end
  end

  def parse(row_content, columns_specs) do
    columns_specs
    |> Enum.zip(row_content)
    |> Enum.map(&parse/1)
  end

  def parse({{_,_,_,{@varchar, nil}}, <<data::binary>>}), do: data
  def parse({{_,_,_,{@int, nil}}, data}) do
    len = bit_size(data)
    <<n::integer-size(len)>> = data
    n
  end
end
