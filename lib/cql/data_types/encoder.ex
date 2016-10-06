defmodule CQL.DataTypes.Encoder do
  import CQL.Encoder
  require Bitwise

  def encode(value, type) when is_integer(type) do
    encode(value, CQL.DataTypes.kind(type))
  end

  def encode(value, type) do
    type
    |> parts(value)
    |> Enum.join
  end

  defp parts(:ascii,     value), do: [bytes(value)]
  defp parts(:bigint,    value), do: [int(8), long(value)]
  defp parts(:blob,      value), do: [bytes(value)]
  defp parts(:boolean,   true),  do: [int(1), byte(1)]
  defp parts(:boolean,   false), do: [int(1), byte(0)]
  defp parts(:counter,   value), do: [int(8), long(value)]
  defp parts(:double,    value), do: [int(8), double(value)]
  defp parts(:float,     value), do: [int(4), float(value)]
  defp parts(:int,       value), do: [int(4), int(value)]
  defp parts(:timestamp, value), do: [int(8), long(value)]
  defp parts(:text,      value), do: [long_string(value)]
  defp parts(:uuid,      value), do: [int(16), uuid(value)]
  defp parts(:varchar,   value), do: [long_string(value)]
  defp parts(:time,      value), do: [int(8), long(value)]
  defp parts(:smallint,  value), do: [int(2), short(value)]
  defp parts(:tinyint,   value), do: [int(1), tinyint(value)]
  defp parts(:inet,      value), do: [value] #TODO
  defp parts(:date,      value), do: [CQL.DataTypes.Date.encode(value)]
  defp parts(:varint,    value), do: [varint(value)]

  defp parts(:decimal,   {unscaled, scale}) do
    [int(scale), varint(unscaled)]
  end

  defp varint(n) do
    bytes = int_bytes(n)
    bits = bytes * 8
    [int(bytes), <<n::signed-integer-size(bits)>>]
  end

  defp int_bytes(x, acc \\ 0)
  defp int_bytes(x, acc) when x >  127 and x <   256, do: acc + 2
  defp int_bytes(x, acc) when x <= 127 and x >= -128, do: acc + 1
  defp int_bytes(x, acc) when x < -128 and x >= -256, do: acc + 2
  defp int_bytes(x, acc), do: int_bytes(Bitwise.bsr(x, 8), acc + 1)
end
