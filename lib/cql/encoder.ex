defmodule CQL.Encoder do

  def byte(n) do
    <<n::unsigned-integer-size(8)>>
  end

  def int(n) do
    <<n::unsigned-integer-size(32)>>
  end

  def long(n) do
    <<n::unsigned-integer-size(64)>>
  end

  def short(n) do
    <<n::unsigned-integer-size(16)>>
  end

  def string(str) do
    (str |> String.length |> short) <> <<str::binary>>
  end

  def long_string(str) do
    (str |> String.length |> int) <> <<str::binary>>
  end

  def uuid(str) do
    <<str::size(16)>>
  end

  def string_list(list) do
    len = Enum.count(list)
    binary =
      list
      |> Enum.map(&string/1)
      |> Enum.join

    <<len::size(16), binary::binary>>
  end

  def bytes(nil) do
    int(-1)
  end

  def bytes(str) do
    int(byte_size(str)) <> <<str::binary>>
  end

  def short_bytes(str) do
    short(byte_size(str)) <> <<str::binary>>
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
    binary =
      map
      |> Enum.map(fn {k, v} -> string(k) <> string(v) end)
      |> Enum.join

    short(len) <> <<binary::binary>>
  end

  def string_multimap(map) do
    map
    |> Enum.map(fn {k, v} -> {k, string_list(v)} end)
    |> string_map
  end

  def bytes_map(map) do
    len = Enum.count(map)
    binary =
      map
      |> Enum.map(fn {k, v} -> string(k) <> bytes(v) end)
      |> Enum.join

    short(len) <> <<binary::binary>>
  end

  def prepend(list, item), do: [item | list]
  def prepend_when(list, item, true), do: [item | list]
  def prepend_when(list, _item, false), do: list
end
