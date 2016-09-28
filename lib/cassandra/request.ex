defmodule Cassandra.Request do
  alias Cassandra.Frame
  alias Cassandra.Frame.{Encoder, Query}

  def startup do
    request(:STARTUP, Encoder.string_map(%{"CQL_VERSION" => "3.0.0"}))
  end

  def options do
    request(:OPTIONS)
  end

  def query(query, flags \\ 0, consistency \\ :ANY) do
    request(:QUERY, Encoder.encode(%Query{query: query, flags: flags, consistency: consistency}))
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
