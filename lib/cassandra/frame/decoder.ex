defmodule Cassandra.Frame.Decoder do
  alias Cassandra.Frame
  alias Cassandra.Frame.{Opration, Consistency}

  def decode(<<
      version::unsigned-integer-size(8),
      flags::unsigned-integer-size(8),
      stream::signed-integer-size(16),
      opcode::unsigned-integer-size(8),
      length::unsigned-integer-size(32),
      body::binary-size(length),
    >>)
  do
    frame = %Frame{
      version: version,
      flags: flags,
      stream: stream,
      opration: opration(opcode),
      length: length,
      body: body,
    }
    case frame.opration do
      :ERROR ->
        {:ok, %{frame | body: error(frame.body)}}
      :RESULT ->
        {:ok, %{frame | body: result(frame.body)}}
      :SUPPORTED ->
        {:ok, %{frame | body: supported(body)}}
      _ ->
        {:ok, frame}
    end
  end

  def decode(_) do
    :error
  end

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

  def string_list({n, binary}) do
    Enum.reduce 1..n, {[], binary}, fn(_, {list, rest}) ->
      {str, rest} = string(rest)
      {[str | list], rest}
    end
  end

  def string_list(binary) do
    binary |> short |> string_list
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

  def opration(code) do
    Opration.name(code)
  end

  def error(body) do
    {code, rest} = int(body)
    {message, ""} = string(rest)

    %{code: code, message: message}
  end

  def result(body) do
    {kind, rest} = int(body)
    case kind do
      0x01 -> void(rest)
      0x02 -> rows(rest)
      0x03 -> set_keyspace(rest)
      0x04 -> prepared(rest)
      0x05 -> schema_change(rest)
    end
  end

  def void(_), do: %{}
  def rows(rest), do: rest
  def set_keyspace(rest), do: rest
  def prepared(rest), do: rest
  def schema_change(rest), do: rest

  def supported(body) do
    {body, ""} = string_multimap(body)
    body
  end
end
