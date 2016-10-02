defmodule CQL.Ready do
  defstruct []

  def decode(%CQL.Frame{}) do
    %__MODULE__{}
  end
end
