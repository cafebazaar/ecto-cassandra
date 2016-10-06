defmodule CQL.Execute do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, Frame, QueryParams}

  defstruct [
    :id,
    :params,
  ]

  defimpl Request do
    def frame(%CQL.Execute{id: id, params: %QueryParams{} = params}) do
      %Frame{
        opration: :EXECUTE,
        body: short_bytes(id) <> QueryParams.encode(params),
      }
    end
  end
end
