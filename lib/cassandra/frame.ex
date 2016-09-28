defmodule Cassandra.Frame do
  defstruct [
    version: 0x03,
    flags: 0x00,
    stream: 0,
    opration: 0,
    length: 0,
    body: <<>>,
  ]

  defdelegate encode(frame), to: Cassandra.Frame.Encoder
  defdelegate decode(binary), to: Cassandra.Frame.Decoder
end
