defmodule CQL.Query do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, QueryParams}

  defstruct [
    query: "",
    params: %QueryParams{},
  ]

  defimpl Request do
    def encode(%CQL.Query{query: query, params: %QueryParams{} = params}) do
      with {:ok, encoded_query} <- ok(long_string(query)),
           {:ok, encoded_params} <- ok(QueryParams.encode(params))
      do
        {:QUERY, encoded_query <> encoded_params}
      end
    end

    def encode(_), do: :error
  end
end
