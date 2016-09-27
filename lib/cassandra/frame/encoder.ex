defmodule Cassandra.Frame.Encoder do

  alias Cassandra.Frame

  def encode(f = %Frame{}) do
    <<f.version::unsigned-integer-size(8),
      f.flags::unsigned-integer-size(8),
      f.stream::signed-integer-size(16),
      Frame.codes[f.opration]::unsigned-integer-size(8),
      byte_size(f.body)::unsigned-integer-size(32),
      f.body::binary,
    >>
  end

  def string(str) do
    <<String.length(str)::unsigned-integer-size(16), str::binary>>
  end

  def long_string(str) do
    <<String.length(str)::unsigned-integer-size(32), str::binary>>
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
end
