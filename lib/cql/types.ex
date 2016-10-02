defmodule CQL.Types do
  alias CQL.{Encoder, Decoder}

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

  def decode(type, data) do
    {value, ""} = parse(type, data)
    value
  end

  def parse({@varchar, nil}, data) do
    Decoder.string(data)
  end

  def parse({@int, nil}, data) do
    Decoder.int(data)
  end

  def parse({@varint, nil}, data) do
    len = bit_size(data)
    <<n::integer-size(len)>> = data
    n
  end

  def encode(bit) when is_boolean(bit) do
    Encoder.int(1) <> Encoder.byte(bit)
  end

  def encode(data) when is_integer(data) do
    Encoder.int(4) <> Encoder.int(data)
  end

  def encode(data) when is_bitstring(data) do
    Encoder.long_string(data)
  end
end
