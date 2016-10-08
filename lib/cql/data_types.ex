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

  def kind({id, nil}), do: kind(id)
  def kind({id, value}), do: {kind(id), value}
  def kind(id) when is_integer(id) do
    Map.fetch!(@kinds, id)
  end

  defdelegate encode(sepc), to: CQL.DataTypes.Encoder
  defdelegate encode(value, type), to: CQL.DataTypes.Encoder

  defdelegate decode(spec), to: CQL.DataTypes.Decoder
  defdelegate decode(value, type), to: CQL.DataTypes.Decoder
end
