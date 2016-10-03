defmodule CQL.Result.SchemaChange do
  import CQL.Decoder

  defstruct [
    :change_type,
    :target,
    :options,
  ]

  def decode(buffer) do
    {data, ""} = unpack buffer,
      change_type: &string/1,
      target:      &string/1,
      keyspace:    {&string/1, :target, &(&1 != "KEYSPACE")},
      name:        &string/1

    options = if data.target != "KEYSPACE" do
      {data.keyspace, data.name}
    else
      data.name
    end

    %__MODULE__{
      change_type: data.change_type,
      target: data.target,
      options: options,
    }
  end
end
