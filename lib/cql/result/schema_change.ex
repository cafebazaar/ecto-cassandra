defmodule CQL.Result.SchemaChange do
  import CQL.Decoder

  defstruct [
    :change_type,
    :target,
    :options,
  ]

  def decode(binary) do
    {change_type, x} = string(binary)
    {target,      x} = string(x)
    {keyspace,    x} = run_when(&string/1, x, target != "KEYSPACE")
    {name,       ""} = string(x)

    options = if target != "KEYSPACE" do
      {keyspace, name}
    else
      name
    end

    %__MODULE__{
      change_type: change_type,
      target: target,
      options: options,
    }
  end
end
