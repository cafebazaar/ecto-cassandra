defmodule Cassandra.Frame.Consistency do
  @codes %{
    :ANY          => 0x00,
    :ONE          => 0x01,
    :TWO          => 0x02,
    :THREE        => 0x03,
    :QUORUM       => 0x04,
    :ALL          => 0x05,
    :LOCAL_QUORUM => 0x06,
    :EACH_QUORUM  => 0x07,
    :SERIAL       => 0x08,
    :LOCAL_SERIAL => 0x09,
    :LOCAL_ONE    => 0x0A,
  }

  def code(name) do
    Map.fetch!(@codes, name)
  end
end
