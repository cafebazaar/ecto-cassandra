defmodule CQL.DataTypes.Encoder do
  require Bitwise

  def encode(nil),                            do: encode({nil, nil})
  def encode(%Date{} = value),                do: encode({:date, value})
  def encode(value) when is_integer(value),   do: encode({:int, value})
  def encode(value) when is_float(value),     do: encode({:double, value})
  def encode(value) when is_bitstring(value), do: encode({:text, value})
  def encode(value) when is_boolean(value),   do: encode({:boolean, value})

  def encode({type, value}), do: encode(value, type)

  def encode(value, type), do: type |> enc(value) |> bytes

  def byte(n) when is_integer(n), do: <<n::integer-8>>
  def byte(_), do: :error

  def boolean(false), do: byte(0)
  def boolean(true),  do: byte(1)
  def boolean(_),     do: :error

  def tinyint(n) when is_integer(n), do: <<n::signed-integer-8>>
  def tinyint(_), do: :error

  def short(n) when is_integer(n), do: <<n::integer-16>>
  def short(_), do: :error

  def int(n) when is_integer(n), do: <<n::signed-integer-32>>
  def int(_), do: :error

  def long(n) when is_integer(n), do: <<n::signed-integer-64>>
  def long(_), do: :error

  def float(x) when is_float(x), do: <<x::float-32>>
  def float(_), do: :error

  def double(x) when is_float(x), do: <<x::float-64>>
  def double(_), do: :error

  def string(str) when is_bitstring(str), do: (str |> String.length |> short) <> <<str::bytes>>
  def string(_), do: :error

  def long_string(str) when is_bitstring(str), do: (str |> String.length |> int) <> <<str::bytes>>
  def long_string(_), do: :error

  def uuid(str) when is_bitstring(str), do: UUID.string_to_binary!(str)
  def uuid(_), do: :error

  def string_list(list) when is_list(list) do
    if Enum.all?(list, &is_bitstring/1) do
      n = Enum.count(list)
      buffer = list |> Enum.map(&string/1) |> Enum.join
      short(n) <> <<buffer::bytes>>
    else
      :error
    end
  end

  def bytes(nil), do: int(-1)
  def bytes(bytes) when is_binary(bytes), do: int(byte_size(bytes)) <> <<bytes::bytes>>
  def bytes(_), do: :error

  def short_bytes(nil), do: int(-1)
  def short_bytes(bytes) when is_binary(bytes), do: short(byte_size(bytes)) <> <<bytes::bytes>>
  def short_bytes(_), do: :error

  def inet(ip) when is_tuple(ip), do: ip |> Tuple.to_list |> inet
  def inet(ip) when is_list(ip), do: ip |> Enum.map(&byte/1) |> Enum.join
  def inet(_), do: :error

  def string_map(map) when is_map(map) do
    if map |> Map.values |> Enum.all?(&is_bitstring/1) do
      n = Enum.count(map)
      buffer = map |> Enum.map(fn {k, v} -> string(k) <> string(v) end) |> Enum.join
      short(n) <> <<buffer::bytes>>
    else
      :error
    end
  end

  def string_map(_), do: :error

  def string_multimap(map) when is_map(map) do
    map
    |> Enum.map(fn {k, v} -> {k, string_list(v)} end)
    |> string_map
  end

  def string_multimap(_), do: :error

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

    int(size) <> <<buffer::bytes>>
  end

  def map(map, {ktype, vtype}) do
    size = Enum.count(map)
    buffer =
      map
      |> Enum.map(fn {k, v} -> encode(k, ktype) <> encode(v, vtype) end)
      |> Enum.join

    int(size) <> <<buffer::bytes>>
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
  def prepend(list, _, false), do: list
  def prepend(list, item, true), do: [item | list]
  def prepend(list, _, nil), do: list
  def prepend(list, item, _), do: [item | list]
  def prepend_not_nil(list, nil, _func), do: list
  def prepend_not_nil(list, item, func), do: [apply(__MODULE__, func, [item]) | list]

  def ok(:error), do: :error
  def ok(value),  do: {:ok, value}

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
