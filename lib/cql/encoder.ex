defmodule CQL.Encoder do

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

  def inet(xs, port) do
    address = xs |> Enum.map(&byte/1) |> Enum.join
    Enum.join([byte(length(xs)), address, int(port)])
  end

  def inet(str) do
    if String.contains?(str, ".") do
      [address, port] = String.split(str, ":")
      address |> String.split(".") |> inet(port)
    else
      [port | address] =
        str |> String.split(":") |> Enum.reverse
      address |> Enum.reverse |> inet(port)
    end
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
    len = Enum.count(map)
    buffer =
      map
      |> Enum.map(fn {k, v} -> string(k) <> bytes(v) end)
      |> Enum.join

    short(len) <> <<buffer::bytes>>
  end

  def consistency(name) do
    name
    |> CQL.Consistency.code
    |> short
  end

  def prepend(list, item), do: [item | list]
  def prepend_when(list, item, true), do: [item | list]
  def prepend_when(list, _item, false), do: list
end
