defmodule CQL.Startup do
  import CQL.DataTypes.Encoder

  alias CQL.{Request, Frame}

  defstruct [options: %{"CQL_VERSION" => "3.0.0"}]

  defimpl Request do
    def frame(%CQL.Startup{options: options}) do
      %Frame{
        opration: :STARTUP,
        body: string_map(options),
      }
    end
  end
end
