defmodule CQL.Query do
  import CQL.Encoder
  alias CQL.{Request, Frame, QueryParams}

  defstruct [
    query: "",
    params: %QueryParams{consistency: :ONE, flags: 0},
  ]

  defimpl Request do
    def frame(%CQL.Query{query: query, params: %QueryParams{} = params}) do
      %Frame{
        opration: :QUERY,
        body: long_string(query) <> QueryParams.encode(params),
      }
    end
  end
end
