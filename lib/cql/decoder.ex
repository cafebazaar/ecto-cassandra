defmodule CQL.Decoder do
  require Bitwise

  def byte(<<n::unsigned-integer-size(8), rest::binary>>), do: {n, rest}

  def tinyint(<<n::unsigned-integer-size(8), rest::binary>>), do: {n, rest}

  def short(<<n::unsigned-integer-size(16), rest::binary>>), do: {n, rest}

  def int(<<n::signed-integer-size(32), rest::binary>>), do: {n, rest}

  def long(<<n::signed-integer-size(64), rest::binary>>), do: {n, rest}

  def float(<<x::float-size(32), rest::binary>>), do: {x, rest}

  def double(<<x::float-size(64), rest::binary>>), do: {x, rest}

  def string({len, buffer}) do
    <<str::binary-size(len), rest::binary>> = buffer
    {str, rest}
  end

  def string(buffer) do
    buffer |> short |> string
  end

  def long_string(buffer) do
    buffer |> long |> string
  end

  def uuid(<<uuid::bitstring-size(128), rest::binary>>) do
    {UUID.binary_to_string!(uuid), rest}
  end

  def string_list({n, buffer}) do
    Enum.reduce 1..n, {[], buffer}, fn(_, {list, rest}) ->
      {str, rest} = string(rest)
      {[str | list], rest}
    end
  end

  def string_list(buffer) do
    buffer |> short |> string_list
  end

  def bytes({len, buffer}) do
    <<str::binary-size(len), rest::binary>> = buffer
    {str, rest}
  end

  def bytes(buffer) do
    buffer |> int |> bytes
  end

  def short_bytes(buffer) do
    buffer |> short |> bytes
  end

  def string_map({n, buffer}) do
    Enum.reduce 1..n, {[], buffer}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = string(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def string_map(buffer) do
    buffer |> short |> string_map
  end

  def string_multimap({n, buffer}) do
    Enum.reduce 1..n, {[], buffer}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = string_list(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def string_multimap(buffer) do
    buffer |> short |> string_multimap
  end

  def bytes_map({n, buffer}) do
    Enum.reduce 1..n, {[], buffer}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = bytes(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def bytes_map(buffer) do
    buffer |> short |> bytes_map
  end

  def matches(flag, flags) do
    Bitwise.band(flag, flags) == flag
  end

  def ntimes(n, func, buffer) do
    ntimes(n, func, buffer, [])
  end

  def ntimes(0, _, buffer, items) do
    {Enum.reverse(items), buffer}
  end

  def ntimes(n, func, buffer, items) do
    {item, buffer} = func.(buffer)
    ntimes(n - 1, func, buffer, [item | items])
  end

  def unpack(buffer, meta) do
    Enum.reduce(meta, {%{}, buffer}, &unpack_item/2)
  end

  def unpack_item({name, {func, key, predicate}}, {map, buffer}) do
    unpack_item({name, {func, [when: predicate.(Map.get(map, key))]}}, {map, buffer})
  end

  def unpack_item({_, {_, [when: false]}}, {map, buffer}) do
    {map, buffer}
  end

  def unpack_item({name, {func, [when: true]}}, {map, buffer}) do
    unpack_item({name, func}, {map, buffer})
  end

  def unpack_item({name, {func, [when: flag]}}, {map, buffer}) do
    unpack_item({name, {func, [when: matches(flag, map.flags)]}}, {map, buffer})
  end

  def unpack_item({name, {func, [unless: boolean]}}, {map, buffer}) when is_boolean(boolean) do
    unpack_item({name, {func, [when: !boolean]}}, {map, buffer})
  end

  def unpack_item({name, func}, {map, buffer}) do
    {value, buffer} = func.(buffer)
    {Map.put(map, name, value), buffer}
  end

  def consistency(buffer) do
    {code, buffer} = short(buffer)
    {CQL.Consistency.name(code), buffer}
  end
end
