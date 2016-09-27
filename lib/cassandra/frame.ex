defmodule Cassandra.Frame do
  defstruct [
    version: 0x03,
    flags: 0x00,
    stream: 0,
    opration: 0,
    length: 0,
    body: <<>>,
  ]

  @names %{
    0x00 => :ERROR,
    0x01 => :STARTUP,
    0x02 => :READY,
    0x03 => :AUTHENTICATE,
    0x05 => :OPTIONS,
    0x06 => :SUPPORTED,
    0x07 => :QUERY,
    0x08 => :RESULT,
    0x09 => :PREPARE,
    0x0A => :EXECUTE,
    0x0B => :REGISTER,
    0x0C => :EVENT,
    0x0D => :BATCH,
    0x0E => :AUTH_CHALLENGE,
    0x0F => :AUTH_RESPONSE,
    0x10 => :AUTH_SUCCESS,
  }

  @codes %{
    :ERROR          => 0x00,
    :STARTUP        => 0x01,
    :READY          => 0x02,
    :AUTHENTICATE   => 0x03,
    :OPTIONS        => 0x05,
    :SUPPORTED      => 0x06,
    :QUERY          => 0x07,
    :RESULT         => 0x08,
    :PREPARE        => 0x09,
    :EXECUTE        => 0x0A,
    :REGISTER       => 0x0B,
    :EVENT          => 0x0C,
    :BATCH          => 0x0D,
    :AUTH_CHALLENGE => 0x0E,
    :AUTH_RESPONSE  => 0x0F,
    :AUTH_SUCCESS   => 0x10,
  }

  def names, do: @names
  def codes, do: @codes

  defdelegate encode(frame), to: Cassandra.Frame.Encoder
  defdelegate decode(binary), to: Cassandra.Frame.Decoder
end
