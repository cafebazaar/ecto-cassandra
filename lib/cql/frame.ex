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
      version::unsigned-integer-size(8),
      flags::unsigned-integer-size(8),
      stream::signed-integer-size(16),
      opcode::unsigned-integer-size(8),
      length::unsigned-integer-size(32),
      body::binary-size(length),
    >>)
  do
    %__MODULE__{
      version: version,
      flags: flags,
      stream: stream,
      opration: Opration.decode(opcode),
      length: length,
      body: body,
    }
  end
end
