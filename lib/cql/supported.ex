defmodule CQL.Supported do
  import CQL.Decoder

  defstruct [options: %{}]

  def decode(body) do
    {options, ""} = string_multimap(body)

    %__MODULE__{options: options}
  end
end
