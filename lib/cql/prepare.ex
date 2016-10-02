defmodule CQL.Prepare do
  import CQL.Encoder
  alias CQL.{Request, Frame}

  defstruct [
    query: "",
  ]

  defimpl Request do
    def frame(%CQL.Prepare{query: query}) do
      %Frame{
        opration: :PREPARE,
        body: long_string(query),
      }
    end
  end
end
