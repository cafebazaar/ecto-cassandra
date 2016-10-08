defmodule CQL.Result.Rows do
  import CQL.DataTypes.Decoder

  defstruct [:metadata, :data]

  def decode(buffer) do
    {meta, buffer} = unpack buffer,
      metadata:   &CQL.MetaData.decode/1,
      rows_count: :int

    {data, ""} = ntimes(meta.rows_count, row_content(meta.metadata), buffer)

    %__MODULE__{metadata: meta.metadata, data: data}
  end

  def to_map(%__MODULE__{metadata: metadata, data: data}) do
    keys = metadata.columns_specs |> Enum.map(&Map.get(&1, :name))
    data
    |> Enum.map(&to_map(keys, &1))
  end

  defp to_map(keys, values) do
    keys
    |> Enum.zip(values)
    |> Enum.into(%{})
  end

  defp row_content(metadata) do
    types = metadata.columns_specs |> Enum.map(&Map.get(&1, :type))
    fn binary ->
      {row, rest} = ntimes(metadata.columns_count, &bytes/1, binary)
      {parse(row, types), rest}
    end
  end

  defp parse(row_content, types) do
    types
    |> Enum.zip(row_content)
    |> Enum.map(&CQL.DataTypes.decode/1)
  end
end
