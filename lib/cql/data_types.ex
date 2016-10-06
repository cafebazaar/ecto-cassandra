defmodule CQL.DataTypes do

  @kinds %{
    0x01 => :ascii,
    0x02 => :bigint,
    0x03 => :blob,
    0x04 => :boolean,
    0x05 => :counter,
    0x06 => :decimal,
    0x07 => :double,
    0x08 => :float,
    0x09 => :int,
    0x0B => :timestamp,
    0x0C => :uuid,
    0x0D => :varchar,
    0x0E => :varint,
    0x0F => :timeuuid,
    0x10 => :inet,
    0x11 => :date,
    0x12 => :time,
    0x13 => :smallint,
    0x14 => :tinyint,
  }

  def kind(type) do
    Map.fetch!(@kinds, type)
  end

  defdelegate encode(sepc), to: CQL.DataTypes.Encoder
  defdelegate encode(value, type), to: CQL.DataTypes.Encoder

  defdelegate decode(spec), to: CQL.DataTypes.Decoder
  defdelegate decode(value, type), to: CQL.DataTypes.Decoder

  # def encode(nil),                            do: encode(nil, nil)
  # def encode(%Date{} = value),                do: encode(value, :date)
  # def encode(value) when is_integer(value),   do: encode(value, :int)
  # def encode(value) when is_float(value),     do: encode(value, :double)
  # def encode(value) when is_bitstring(value), do: encode(value, :varchar)
  # def encode(value) when is_boolean(value),   do: encode(value, :boolean)
end
