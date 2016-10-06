defmodule CQL.Register do
  import CQL.Encoder
  alias CQL.{Request, Frame}

  @types [
    :TOPOLOGY_CHANGE,
    :STATUS_CHANGE,
    :SCHEMA_CHANGE,
  ]

  defstruct [types: @types]

  defimpl Request do
    def frame(%CQL.Register{types: types}) do
      %Frame{
        opration: :REGISTER,
        body: string_list(types),
      }
    end
  end
end
