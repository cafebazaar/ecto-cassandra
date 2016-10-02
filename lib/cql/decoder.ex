defmodule CQL.Decoder do
  require Bitwise

  def byte(<<n::unsigned-integer-size(8), rest::binary>>), do: {n, rest}

  def int(<<n::signed-integer-size(32), rest::binary>>), do: {n, rest}

  def long(<<n::signed-integer-size(64), rest::binary>>), do: {n, rest}

  def short(<<n::unsigned-integer-size(16), rest::binary>>), do: {n, rest}

  def string({len, binary}) do
    <<str::binary-size(len), rest::binary>> = binary
    {str, rest}
  end

  def string(binary) do
    binary |> short |> string
  end

  def long_string(binary) do
    binary |> long |> string
  end

  def uuid(<<uuid::binary-size(128), rest::binary>>), do: {uuid, rest}

  def string_list({n, binary}) do
    Enum.reduce 1..n, {[], binary}, fn(_, {list, rest}) ->
      {str, rest} = string(rest)
      {[str | list], rest}
    end
  end

  def string_list(binary) do
    binary |> short |> string_list
  end

  def bytes({len, binary}) do
    <<str::binary-size(len), rest::binary>> = binary
    {str, rest}
  end

  def bytes(binary) do
    binary |> int |> bytes
  end

  def short_bytes(binary) do
    binary |> short |> bytes
  end

  def string_map({n, binary}) do
    Enum.reduce 1..n, {[], binary}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = string(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def string_map(binary) do
    binary |> short |> string_map
  end

  def string_multimap({n, binary}) do
    Enum.reduce 1..n, {[], binary}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = string_list(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def string_multimap(binary) do
    binary |> short |> string_multimap
  end

  def bytes_map({n, binary}) do
    Enum.reduce 1..n, {[], binary}, fn(_, {kvs, rest}) ->
      {key, rest} = string(rest)
      {value, rest} = bytes(rest)
      {[{key, value} | kvs], rest}
    end
  end

  def bytes_map(binary) do
    binary |> short |> bytes_map
  end

  def matches(flag, flags) do
    Bitwise.band(flag, flags) == flag
  end

  def run_when(func, binary, true), do: func.(binary)
  def run_when(_, binary, false), do: {nil, binary}

  def run_when_matches(func, binary, flag, flags) do
    run_when(func, binary, matches(flag, flags))
  end

  def ntimes(n, func, binary, items \\ [])

  def ntimes(0, _, rest, items) do
    {Enum.reverse(items), rest}
  end

  def ntimes(n, func, binary, items) do
    {item, rest} = func.(binary)
    ntimes(n - 1, func, rest, [item | items])
  end
end
