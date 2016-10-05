defmodule CQL.Consistency do
  @codes %{
    :any          => 0x00,
    :one          => 0x01,
    :two          => 0x02,
    :three        => 0x03,
    :quorum       => 0x04,
    :all          => 0x05,
    :local_quorum => 0x06,
    :each_quorum  => 0x07,
    :serial       => 0x08,
    :local_serial => 0x09,
    :local_one    => 0x0A,
  }

  @names @codes
    |> Enum.map(fn {x, y} -> {y, x} end)
    |> Enum.into(%{})

  def code(name) do
    Map.fetch!(@codes, name)
  end

  def name(code) do
    Map.fetch!(@names, code)
  end
end
