defmodule Cassandra.Request do
  alias Cassandra.Frame

  def startup do
    request(:STARTUP, Frame.Encoder.string_map(%{"CQL_VERSION" => "3.0.0"}))
  end

  def options do
    request(:OPTIONS)
  end

  defp request(opration, body \\ <<>>, stream \\ 0, flags \\ 0) do
    %Frame{
      version: 0x04,
      opration: opration,
      flags: flags,
      stream: stream,
      body: body,
    }
  end
end
