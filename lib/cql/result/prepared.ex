defmodule CQL.Result.Prepared do
  import CQL.Decoder

  alias CQL.MetaData

  defstruct [
    :id,
    :metadata,
    :result_metadata,
  ]

  def decode(binary) do
    {id,              x} = short_bytes(binary)
    {metadata,        x} = MetaData.decode(x, true)
    {result_metadata, x} = run_when(&MetaData.decode/1, x, !is_nil(metadata.columns_specs))

    %__MODULE__{
      id: id,
      metadata: metadata,
      result_metadata: result_metadata,
    }
  end
end
