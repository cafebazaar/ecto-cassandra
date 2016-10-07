defmodule CQL.DataTypes.Encoder do
  require Bitwise

  def encode(nil),                            do: encode({nil, nil})
  def encode(%Date{} = value),                do: encode({:date, value})
  def encode(value) when is_integer(value),   do: encode({:int, value})
  def encode(value) when is_float(value),     do: encode({:double, value})
  def encode(value) when is_bitstring(value), do: encode({:text, value})
  def encode(value) when is_boolean(value),   do: encode({:boolean, value})

  def encode({type, value}) do
    encode(value, type)
  end

  def encode(value, type) when is_integer(type) do
    encode(value, CQL.DataTypes.kind(type))
  end

  def encode(value, type) do
    type |> enc(value) |> bytes
  end

  def byte(n) do
    <<n::integer-8>>
  end

  def boolean(false), do: byte(0)
  def boolean(true),  do: byte(1)

  def tinyint(n) do
    <<n::signed-integer-8>>
  end

  def short(n) do
    <<n::integer-16>>
  end

  def int(n) do
    <<n::signed-integer-32>>
  end

  def long(n) do
    <<n::signed-integer-64>>
  end

  def float(x) do
    <<x::float-32>>
  end

  def double(x) do
    <<x::float-64>>
  end

  def string(str) do
    (str |> String.length |> short) <> <<str::bytes>>
  end

  def long_string(str) do
    (str |> String.length |> int) <> <<str::bytes>>
  end

  def uuid(str) do
    UUID.string_to_binary!(str)
  end

  def string_list(list) do
    len = Enum.count(list)
    buffer =
      list
      |> Enum.map(&string/1)
      |> Enum.join

    short(len) <> <<buffer::bytes>>
  end

  def bytes(nil) do
    int(-1)
  end

  def bytes(bytes) do
    int(byte_size(bytes)) <> <<bytes::bytes>>
  end

  def short_bytes(bytes) do
    short(byte_size(bytes)) <> <<bytes::bytes>>
  end

  def inet({ip, port}) when is_tuple(ip) do
    ip |> Tuple.to_list |> inet(port)
  end

  def inet(ip, port) do
    address = ip |> Enum.map(&byte/1) |> Enum.join
    Enum.join([byte(length(ip)), address, int(port)])
  end

  def string_map(map) do
    len = Enum.count(map)
    buffer =
      map
      |> Enum.map(fn {k, v} -> string(k) <> string(v) end)
      |> Enum.join

    short(len) <> <<buffer::bytes>>
  end

  def string_multimap(map) do
    map
    |> Enum.map(fn {k, v} -> {k, string_list(v)} end)
    |> string_map
  end

  def bytes_map(map) do
    size = Enum.count(map)
    buffer =
      map
      |> Enum.map(fn {k, v} -> string(k) <> bytes(v) end)
      |> Enum.join

    short(size) <> <<buffer::bytes>>
  end

  def list(list, type) do
    size = Enum.count(list)
    buffer =
      list
      |> Enum.map(&encode(&1, type))
      |> Enum.join

    short(size) <> <<buffer::bytes>>
  end

  def map(map, {ktype, vtype}) do
    size = Enum.count(map)
    buffer =
      map
      |> Enum.map(fn {k, v} -> encode(k, ktype) <> encode(v, vtype) end)
      |> Enum.join

    short(size) <> <<buffer::bytes>>
  end

  def set(set, type) do
    set |> MapSet.to_list |> list(type)
  end

  def tuple(tuple, types) do
    list = Tuple.to_list(tuple)
    size = Enum.count(list)
    buffer =
      list
      |> Enum.zip(types)
      |> Enum.map(fn {v, t} -> encode(v, t) end)
      |> Enum.join

    short(size) <> <<buffer::bytes>>
  end

  def varint(n) do
    bytes = int_bytes(n)
    bits = bytes * 8
    int(bytes) <> <<n::signed-integer-size(bits)>>
  end

  def decimal({unscaled, scale}) do
    int(scale) <> varint(unscaled)
  end

  def consistency(name) do
    name
    |> CQL.Consistency.code
    |> short
  end

  ### Helpers ###

  def prepend(list, item), do: [item | list]
  def prepend(list, _, nil), do: list
  def prepend(list, _, false), do: list
  def prepend(list, item, true), do: [item | list]
  def prepend_not_nil(list, nil), do: list
  def prepend_not_nil(list, item), do: [item | list]

  def now(unit), do: :erlang.system_time(unit)

  ### Utils ###

  defp int_bytes(x, acc \\ 0)
  defp int_bytes(x, acc) when x >  127 and x <   256, do: acc + 2
  defp int_bytes(x, acc) when x <= 127 and x >= -128, do: acc + 1
  defp int_bytes(x, acc) when x < -128 and x >= -256, do: acc + 2
  defp int_bytes(x, acc), do: int_bytes(Bitwise.bsr(x, 8), acc + 1)

  defp enc(_type, nil), do: int(-1)
  defp enc(_type, :not_set), do: int(-2)

  defp enc(:ascii,     value), do: value
  defp enc(:bigint,    value), do: long(value)
  defp enc(:blob,      value), do: value
  defp enc(:boolean,   true),  do: byte(1)
  defp enc(:boolean,   false), do: byte(0)
  defp enc(:counter,   value), do: long(value)
  defp enc(:date,      value), do: CQL.DataTypes.Date.encode(value)
  defp enc(:decimal,   value), do: decimal(value)
  defp enc(:double,    value), do: double(value)
  defp enc(:float,     value), do: float(value)
  defp enc(:inet,      value), do: inet(value)
  defp enc(:int,       value), do: int(value)
  defp enc(:smallint,  value), do: short(value)
  defp enc(:text,      value), do: value
  defp enc(:timestamp, :now ), do: enc(:timestamp, now(:milliseconds))
  defp enc(:timestamp, value), do: long(value)
  defp enc(:time,      :now ), do: enc(:time, now(:nanosecond))
  defp enc(:time,      value), do: long(value)
  defp enc(:timeuuid,  value), do: uuid(value)
  defp enc(:tinyint,   value), do: tinyint(value)
  defp enc(:uuid,      value), do: uuid(value)
  defp enc(:varchar,   value), do: value
  defp enc(:varint,    value), do: varint(value)

  defp enc({:list, type},   value), do: list(value, type)
  defp enc({:map, type},    value), do: map(value, type)
  defp enc({:set, type},    value), do: set(value, type)
  defp enc({:tuple, types}, value), do: tuple(value, types)
end
