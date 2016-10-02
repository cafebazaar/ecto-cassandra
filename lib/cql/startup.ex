defmodule CQL.Startup do
  defstruct [:options]

  import CQL.Encoder
  alias CQL.{Request, Frame}

  defimpl Request do
    def frame(%CQL.Startup{options: options}) do
      %Frame{
        opration: :STARTUP,
        body: string_map(options),
      }
    end
  end
end
