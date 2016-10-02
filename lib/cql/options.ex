defmodule CQL.Options do
  defstruct []

  defimpl CQL.Request do
    def frame(%CQL.Options{}) do
      %CQL.Frame{
        opration: :OPTIONS,
      }
    end
  end
end
