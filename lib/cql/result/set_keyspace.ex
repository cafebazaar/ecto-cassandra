defmodule CQL.Result.SetKeyspace do
  import CQL.Decoder

  defstruct [:name]

  def decode(binary) do
    {keypace, ""} = string(binary)
    %__MODULE__{name: keypace}
  end
end

