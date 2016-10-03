defmodule CQL.QueryParams do
  require Bitwise

  import CQL.Encoder
  alias CQL.Consistency

  defstruct [
    consistency: :ONE,
    flags: 0,
    values: nil,
    result_page_size: nil,
    paging_state: nil,
    serial_consistency: nil,
    timestamp: nil,
  ]

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

  def encode(q = %__MODULE__{}) do
    flags =
      []
      |> prepend_when(:VALUES, !is_nil(q.values))
      |> prepend_when(:WITH_NAMES, is_map(q.values))

    [Consistency.encode(q.consistency)]
    |> prepend(flags |> flag |> byte)
    |> prepend_when(values(q.values), !is_nil(q.values))
    |> Enum.reverse
    |> Enum.join
  end

  def values(nil), do: nil

  def values(list) when is_list(list) do
    n = Enum.count(list)
    binary =
      list
      |> Enum.map(&CQL.Types.encode/1)
      |> Enum.join
    short(n) <> <<binary::binary>>
  end

  def values(map) when is_map(map) do
    n = Enum.count(map)
    binary =
      map
      |> Enum.map(fn {k, v} -> string(to_string(k)) <> CQL.Types.encode(v) end)
      |> Enum.join
    short(n) <> <<binary::binary>>
  end
end
