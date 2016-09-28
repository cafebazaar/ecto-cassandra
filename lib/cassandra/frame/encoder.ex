defmodule Cassandra.Frame.Encoder do
  alias Cassandra.Frame
  alias Cassandra.Frame.{Opration, Consistency, Query}

  def encode(f = %Frame{}) do
    Enum.join [
      byte(f.version),
      byte(f.flags),
      short(f.stream),
      opration(f.opration),
      int(byte_size(f.body)),
      f.body,
    ]
  end

  def encode(q = %Query{}) do
    Enum.join [
      long_string(q.query),
      consistency(q.consistency),
      byte(q.flags),
    ]
  end

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

  def string_map(map) do
    len = Enum.count(map)
    binary =
      map
      |> Enum.map(fn {k, v} -> string(k) <> string(v) end)
      |> Enum.join

    <<len::size(16), binary::binary>>
  end

  def string_list(list) do
    len = Enum.count(list)
    binary =
      list
      |> Enum.map(&string/1)
      |> Enum.join

    <<len::size(16), binary::binary>>
  end

  def string_multimap(map) do
    map
    |> Enum.map(fn {k, v} -> {k, string_list(v)} end)
    |> string_map
  end

  def consistency(name) do
    name |> Consistency.code |> short
  end

  def opration(name) do
    name |> Opration.code |> byte
  end
end
