defmodule CQL.Options do
  defstruct []

  defimpl CQL.Request do
    def encode(%CQL.Options{}) do
      {:OPTIONS, ""}
    end

    def encode(_), do: :error
  end
end
