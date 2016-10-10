defmodule CQL.Startup do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, Startup}

  defstruct [options: %{"CQL_VERSION" => "3.0.0"}]

  defimpl Request do
    def encode(%Startup{options: options}) do
      case string_map(options) do
        :error -> :error
        body   -> {:STARTUP, body}
      end
    end

    def encode(_), do: :error
  end
end
