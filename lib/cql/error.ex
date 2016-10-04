defmodule CQL.Error do
  defstruct [:code, :message, :content]

  import CQL.Decoder

  def decode(buffer) do
    {error, rest} = unpack buffer,
      code:    &int/1,
      message: &string/1

    # TODO: Parse content according to code

    %__MODULE__{code: error.code, message: error.message, content: rest}
  end
end
