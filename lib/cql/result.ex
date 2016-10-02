defmodule CQL.Result do
  import CQL.Decoder

  alias CQL.Result

  def decode(%CQL.Frame{body: body}) do
    {kind, rest} = int(body)
    IO.inspect kind
    case kind do
      0x01 -> Result.Void.decode(rest)
      0x02 -> Result.Rows.decode(rest)
      0x03 -> Result.SetKeyspace.decode(rest)
      0x04 -> Result.Prepared.decode(rest)
      0x05 -> Result.SchemaChange.decode(rest)
    end
  end
end
