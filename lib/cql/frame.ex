defmodule CQL.Frame do
  import CQL.DataTypes.Encoder

  alias CQL.Opration

  defstruct [
    version: 0x04,
    flags: 0x00,
    stream: 0,
    opration: 0,
    length: 0,
    body: "",
  ]

  def encode(f = %__MODULE__{}) do
    Enum.join [
      byte(f.version),
      byte(f.flags),
      short(f.stream),
      Opration.encode(f.opration),
      int(byte_size(f.body)),
      f.body,
    ]
  end

  def decode(<<
      version::integer-8,
      flags::integer-8,
      stream::signed-integer-16,
      opcode::integer-8,
      length::integer-32,
      body::binary-size(length),
      rest::binary,
    >>)
  do
    frame = %__MODULE__{
      version: version,
      flags: flags,
      stream: stream,
      opration: Opration.decode(opcode),
      length: length,
      body: body,
    }
    {frame, rest}
  end

  def decode(buffer), do: {nil, buffer}

  def set_stream_id(<<prefix::bits-16, _::signed-integer-16, rest::binary>>, id) do
    {:ok, <<prefix::bits-16, id::signed-integer-16, rest::binary>>}
  end

  def set_stream_id(_, _), do: :error
end
