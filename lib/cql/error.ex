defmodule CQL.Error do
  defstruct [:code, :message, :content]

  import CQL.Decoder

  def decode(%CQL.Frame{body: body}) do
    {code, rest} = int(body)
    {message, rest} = string(rest)
    
    # TODO: Parse content according to code

    %__MODULE__{code: code, message: message, content: rest}
  end
end
