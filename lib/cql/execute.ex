defmodule CQL.Execute do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, QueryParams}
  alias CQL.Result.Prepared

  defstruct [
    :prepared,
    :params,
  ]

  defimpl Request do
    def encode(%CQL.Execute{prepared: %Prepared{id: id} = prepared, params: %QueryParams{} = params}) do
      with {:ok, zipped} <- ok(zip(prepared.metadata.column_types, params.values)),
           {:ok, encoded_params} <- ok(QueryParams.encode(%{params | values: zipped}))
      do
        {:EXECUTE, short_bytes(id) <> encoded_params}
      end
    end

    def encode(_), do: :error

    defp zip(types, values) when is_map(values) do
      zip_map(types, Enum.to_list(values), [])
    end

    defp zip(types, values) when is_list(values) do
      types
      |> Keyword.values
      |> Enum.zip(values)
    end

    defp zip(_, values) when is_nil(values), do: nil

    defp zip(_, _), do: :error

    defp zip_map(_, [], zipped), do: Enum.into(zipped, %{})

    defp zip_map(types, [{name, value} | values], zipped) do
      case List.keyfind(types, to_string(name), 0) do
        nil ->
          :error
        {_, type} ->
          zip_map(types, values, [{name, {type, value}} | zipped])
      end
    end
  end
end
