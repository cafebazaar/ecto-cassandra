defmodule CQL.DataTypes.Encoder do
  import CQL.Encoder

  def encode(value, type) do
    type
    #|> CQL.DataTypes.kind
    |> parts(value)
    |> Enum.join
  end

  defp parts(:ascii,     value), do: [bytes(value)]
  defp parts(:bigint,    value), do: [int(8), long(value)]
  defp parts(:blob,      value), do: [bytes(value)]
  defp parts(:boolean,   true),  do: [int(1), byte(1)]
  defp parts(:boolean,   false), do: [int(1), byte(0)]
  defp parts(:counter,   value), do: [int(8), long(value)]
  defp parts(:decimal,   value), do: [value] #TODO
  defp parts(:double,    value), do: [int(8), double(value)]
  defp parts(:float,     value), do: [int(4), float(value)]
  defp parts(:int,       value), do: [int(4), int(value)]
  defp parts(:timestamp, value), do: [int(8), long(value)]
  defp parts(:text,      value), do: [string(value)]
  defp parts(:uuid,      value), do: [int(16), uuid(value)]
  defp parts(:varchar,   value), do: [string(value)]
  defp parts(:varint,    value), do: [value] #TODO
  defp parts(:inet,      value), do: [value] #TODO
  defp parts(:date,      value), do: [value] #TODO
  defp parts(:time,      value), do: [int(8), long(value)]
  defp parts(:smallint,  value), do: [int(2), short(value)]
  defp parts(:tinyint,   value), do: [int(1), tinyint(value)]
end
