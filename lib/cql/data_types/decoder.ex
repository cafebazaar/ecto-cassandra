defmodule CQL.DataTypes.Decoder do
  import CQL.Decoder

  def decode(buffer, type) when is_integer(type) do
    type
    |> CQL.DataTypes.kind
    |> decode(buffer)
  end

  def decode(buffer, :ascii    ), do: bytes(buffer)
  def decode(buffer, :bigint   ), do: with_size(8, &long/1, buffer)
  def decode(buffer, :blob     ), do: bytes(buffer)
  def decode(buffer, :boolean  ), do: with_size(1, &boolean/1, buffer)
  def decode(buffer, :counter  ), do: with_size(8, &long/1, buffer)
  def decode(buffer, :decimal  ), do: buffer #TODO
  def decode(buffer, :double   ), do: with_size(8, &double/1, buffer)
  def decode(buffer, :float    ), do: with_size(4, &float/1, buffer)
  def decode(buffer, :int      ), do: with_size(4, &int/1, buffer)
  def decode(buffer, :timestamp), do: with_size(8, &long/1, buffer)
  def decode(buffer, :text     ), do: string(buffer)
  def decode(buffer, :uuid     ), do: with_size(16, &uuid/1, buffer)
  def decode(buffer, :varchar  ), do: string(buffer)
  def decode(buffer, :varint   ), do: buffer #TODO
  def decode(buffer, :inet     ), do: buffer #TODO
  def decode(buffer, :date     ), do: buffer #TODO
  def decode(buffer, :time     ), do: with_size(8, &long/1, buffer)
  def decode(buffer, :smallint ), do: with_size(2, &short/1, buffer)
  def decode(buffer, :tinyint  ), do: with_size(1, &tinyint/1, buffer)
  
  defp boolean(buffer) do
    {x, buffer} = byte(buffer)
    {x == 1, buffer}
  end

  defp with_size(n, func, buffer) do
    {size, buffer} = int(buffer)
    case size do
      0  -> {nil, buffer}
      ^n -> func.(buffer)
      _  -> raise ArgumentError, "size mismatch"
    end
  end
end
