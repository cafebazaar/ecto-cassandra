defmodule Cassandra.Frame.Query do
  defstruct [
    :query,
    :consistency,
    :flags,
    :values,
    :result_page_size,
    :paging_state,
    :serial_consistency,
    :timestamp,
  ]

  require Bitwise

  @flags %{
    :VALUES                  => 0x01,
    :SKIP_METADATA           => 0x02,
    :PAGE_SIZE               => 0x04,
    :WITH_PAGING_STATE       => 0x08,
    :WITH_SERIAL_CONSISTENCY => 0x10,
    :WITH_DEFAULT_TIMESTAMP  => 0x20,
    :WITH_NAMES              => 0x40,
  }

  def flag(flags) do
    flags
    |> Enum.map(&Map.fetch!(@flags, &1))
    |> Enum.reduce(0, &Bitwise.bor(&1, &2))
  end

  def flags(flag) do
    @flags
    |> Enum.filter(fn {_, code} -> Bitwise.band(code, flag) != 0 end)
    |> Enum.map(&elem(&1, 0))
  end
end
