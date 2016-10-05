defmodule CQL.DataTypes.Decoder do
  import CQL.Decoder

  def decode(buffer, type) when is_integer(type) do
    {value, ""} = decode(buffer, CQL.DataTypes.kind(type))
    value
  end

  def decode(buffer, :ascii    ), do: bytes(buffer)
  def decode(buffer, :bigint   ), do: long(buffer)
  def decode(buffer, :blob     ), do: bytes(buffer)
  def decode(buffer, :boolean  ), do: boolean(buffer)
  def decode(buffer, :counter  ), do: long(buffer)
  def decode(buffer, :decimal  ), do: buffer #TODO
  def decode(buffer, :double   ), do: double(buffer)
  def decode(buffer, :float    ), do: float(buffer)
  def decode(buffer, :int      ), do: int(buffer)
  def decode(buffer, :timestamp), do: long(buffer)
  def decode(buffer, :text     ), do: {buffer, ""}
  def decode(buffer, :uuid     ), do: uuid(buffer)
  def decode(buffer, :varchar  ), do: {buffer, ""}
  def decode(buffer, :varint   ), do: buffer #TODO
  def decode(buffer, :inet     ), do: buffer #TODO
  def decode(buffer, :date     ), do: buffer #TODO
  def decode(buffer, :time     ), do: long(buffer)
  def decode(buffer, :smallint ), do: short(buffer)
  def decode(buffer, :tinyint  ), do: tinyint(buffer)
end
