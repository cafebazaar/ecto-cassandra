defmodule CQL.Register do
  import CQL.DataTypes.Encoder
  alias CQL.Request

  @types [
    "TOPOLOGY_CHANGE",
    "STATUS_CHANGE",
    "SCHEMA_CHANGE",
  ]

  defstruct [types: @types]

  defimpl Request do
    def encode(%CQL.Register{types: types}) do
      case string_list(types) do
        :error -> :error
        body   -> {:REGISTER, body}
      end
    end

    def encode(_), do: :error
  end
end
