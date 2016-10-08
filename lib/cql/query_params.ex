defmodule CQL.QueryParams do
  import CQL.DataTypes.Encoder

  require Bitwise

  defstruct [
    consistency: :one,
    flags: 0,
    values: nil,
    skip_metadata: false,
    page_size: nil,
    paging_state: nil,
    serial_consistency: nil,
    timestamp: nil,
  ]

  @flags %{
    :values                  => 0x01,
    :skip_metadata           => 0x02,
    :page_size               => 0x04,
    :with_paging_state       => 0x08,
    :with_serial_consistency => 0x10,
    :with_default_timestamp  => 0x20,
    :with_names              => 0x40,
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
    has_values = !is_nil(q.values) and !Enum.empty?(q.values)
    has_timestamp = is_integer(q.timestamp) and q.timestamp > 0

    flag =
      []
      |> prepend(:values, has_values)
      |> prepend(:skip_metadata, q.skip_metadata)
      |> prepend(:page_size, q.page_size)
      |> prepend(:with_paging_state, q.paging_state)
      |> prepend(:with_serial_consistency, q.serial_consistency)
      |> prepend(:with_default_timestamp, has_timestamp)
      |> prepend(:with_names, has_values and is_map(q.values))
      |> flag
      |> byte

    q.consistency
    |> consistency
    |> List.wrap
    |> prepend(flag)
    |> prepend(values(q.values), has_values)
    |> prepend_not_nil(q.page_size, :int)
    |> prepend_not_nil(q.paging_state, :bytes)
    |> prepend_not_nil(q.serial_consistency, :consistency)
    |> prepend(q.timestamp, has_timestamp)
    |> Enum.reverse
    |> Enum.join
  end

  def values(nil), do: nil

  def values(list) when is_list(list) do
    n = Enum.count(list)
    binary =
      list
      |> Enum.map(&CQL.DataTypes.encode/1)
      |> Enum.join

    short(n) <> <<binary::binary>>
  end

  def values(map) when is_map(map) do
    n = Enum.count(map)
    binary =
      map
      |> Enum.map(fn {k, v} -> string(to_string(k)) <> CQL.DataTypes.encode(v) end)
      |> Enum.join

    short(n) <> <<binary::binary>>
  end
end
