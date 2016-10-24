defmodule CQL.Result.Rows do
  import CQL.DataTypes.Decoder

  defstruct [
    :columns,
    :rows,
    :rows_count,
    :paging_state,
  ]

  def decode(buffer) do
    {meta, buffer} = unpack buffer,
      metadata:   &CQL.MetaData.decode/1,
      rows_count: :int

    {rows, ""} = ntimes(meta.rows_count, row_content(meta.metadata), buffer)

    result = %__MODULE__{
      columns: Keyword.keys(meta.metadata.column_types),
      rows: rows,
      rows_count: meta.rows_count,
    }

    {result, Map.get(meta.metadata, :paging_state)}
  end

  def to_keyword(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, &zip_to(columns, &1, []))
  end

  def to_map(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, &zip_to(columns, &1, %{}))
  end

  defp zip_to(keys, values, into) do
    keys
    |> Enum.zip(values)
    |> Enum.into(into)
  end

  defp row_content(metadata) do
    types = Keyword.values(metadata.column_types)
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
