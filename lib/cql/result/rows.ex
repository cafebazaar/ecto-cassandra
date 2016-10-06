defmodule CQL.Result.Rows do
  import CQL.DataTypes.Decoder

  def decode(buffer) do
    {data, buffer} = unpack buffer,
      metadata:   &CQL.MetaData.decode/1,
      rows_count: :int

    {rows_content, ""} = ntimes(data.rows_count, row_content(data.metadata), buffer)

    rows_content
  end

  def row_content(metadata) do
    keys  = metadata.columns_specs |> Enum.map(&Map.get(&1, :name))
    types = metadata.columns_specs |> Enum.map(&Map.get(&1, :type))
    fn binary ->
      {row, rest} = ntimes(metadata.columns_count, &bytes/1, binary)
      {parse(row, types, keys), rest}
    end
  end

  def parse(row_content, types, keys) do
    values =
      types
      |> Enum.zip(row_content)
      |> Enum.map(&CQL.DataTypes.decode/1)

    keys
    |> Enum.zip(values)
    |> Enum.into(%{})
  end
end
