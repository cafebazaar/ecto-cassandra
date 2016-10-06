defmodule CQL.Decoder do
  require Bitwise

  def byte(<<n::8, rest::bytes>>) do
    {n, rest}
  end

  def boolean(<<0::8, rest::bytes>>), do: {false, rest}
  def boolean(<<_::8, rest::bytes>>), do: {true, rest}

  def tinyint(<<n::signed-integer-8, rest::bytes>>) do
    {n, rest}
  end

  def short(<<n::integer-16, rest::bytes>>) do
    {n, rest}
  end

  def int(<<n::signed-integer-32, rest::bytes>>) do
    {n, rest}
  end

  def long(<<n::signed-integer-64, rest::bytes>>) do
    {n, rest}
  end

  def float(<<x::float-32, rest::bytes>>) do
    {x, rest}
  end

  def double(<<x::float-64, rest::bytes>>) do
    {x, rest}
  end

  def string({len, buffer}) do
    <<str::bytes-size(len), rest::bytes>> = buffer
    {str, rest}
  end

  def string(buffer) do
    buffer |> short |> string
  end

  def long_string(buffer) do
    buffer |> long |> string
  end

  def uuid(<<uuid::bits-128, rest::bytes>>) do
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
    <<str::bytes-size(len), rest::bytes>> = buffer
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

  def consistency(buffer) do
    {code, buffer} = short(buffer)
    {CQL.Consistency.name(code), buffer}
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
    {item, buffer} = ap(func, buffer)
    ntimes(n - 1, func, buffer, [item | items])
  end

  def unpack(buffer, meta) do
    Enum.reduce(meta, {%{}, buffer}, &pick/2)
  end

  defp pick({name, {func, key, predicate}}, {map, buffer}) do
    pick({name, {func, [when: predicate.(Map.get(map, key))]}}, {map, buffer})
  end

  defp pick({_, {_, [when: false]}}, {map, buffer}) do
    {map, buffer}
  end

  defp pick({name, {func, [when: true]}}, {map, buffer}) do
    pick({name, func}, {map, buffer})
  end

  defp pick({name, {func, [when: flag]}}, {map, buffer}) do
    pick({name, {func, [when: matches(flag, map.flags)]}}, {map, buffer})
  end

  defp pick({name, {func, [unless: boolean]}}, {map, buffer}) when is_boolean(boolean) do
    pick({name, {func, [when: !boolean]}}, {map, buffer})
  end

  defp pick({name, func}, {map, buffer}) do
    {value, buffer} = ap(func, buffer)
    {Map.put(map, name, value), buffer}
  end

  defp ap(func, buffer) when is_atom(func) do
    apply(__MODULE__, func, [buffer])
  end

  defp ap(func, buffer) when is_function(func) do
    func.(buffer)
  end
end
