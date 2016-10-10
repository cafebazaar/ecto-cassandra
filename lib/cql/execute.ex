defmodule CQL.Execute do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, Frame, QueryParams}
  alias CQL.Result.Prepared

  defstruct [
    :prepared,
    :params,
  ]

  defimpl Request do
    def frame(%CQL.Execute{prepared: %Prepared{id: id} = prepared, params: %QueryParams{} = params}) do
      values = zip(prepared.metadata.column_types, params.values)

      %Frame{
        opration: :EXECUTE,
        body: short_bytes(id) <> QueryParams.encode(%{params | values: values}),
      }
    end

    defp zip(types, values) when is_map(values) do
      zip_map(types, Enum.to_list(values), [])
    end

    defp zip(types, values) when is_list(values) do
      types
      |> Keyword.values
      |> Enum.zip(values)
    end

    defp zip_map(_, [], zipped), do: Enum.into(zipped, %{})

    defp zip_map(types, [{name, value} | values], zipped) do
      {_, type} = List.keyfind(types, to_string(name), 0)
      zip_map(types, values, [{name, {type, value}} | zipped])
    end
  end
end
