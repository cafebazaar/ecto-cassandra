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
      values = zip(prepared.metadata.columns_specs, params.values)

      %Frame{
        opration: :EXECUTE,
        body: short_bytes(id) <> QueryParams.encode(%{params | values: values}),
      }
    end

    defp zip(specs, values) when is_map(values) do
      specs
      |> Enum.map(&{Map.fetch!(&1, :name), Map.fetch!(&1, :type)})
      |> Enum.into(%{})
      |> zip_map(Enum.to_list(values), [])
    end

    defp zip(specs, values) when is_list(values) do
      specs
      |> Enum.map(&Map.fetch!(&1, :type))
      |> Enum.zip(values)
    end

    defp zip_map(_, [], zipped), do: Enum.into(zipped, %{})

    defp zip_map(types, [{name, value} | values], zipped) do
      type = Map.fetch!(types, to_string(name))
      zip_map(types, values, [{name, {type, value}} | zipped])
    end
  end
end
