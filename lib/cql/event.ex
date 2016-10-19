defmodule CQL.Event do
  import CQL.DataTypes.Decoder

  defstruct [:type, :info]

  def decode(body) do
    {type, buffer} = string(body)

    info = case type do
      "TOPOLOGY_CHANGE" ->
        {info, ""} = unpack buffer,
          change: :string,
          address: :inet

        info

      "STATUS_CHANGE" ->
        {info, ""} = unpack buffer,
          change: :string,
          address: :inet

        info

      "SCHEMA_CHANGE" ->
        {info, buffer} = unpack buffer,
          change: :string,
          target: :string

        {options, ""} = case info.target do
          "KEYSPACE" ->
            unpack buffer,
              keyspace: :string

          "TABLE" ->
            unpack buffer,
              keyspace: :string,
              table: :string

          "TYPE" ->
            unpack buffer,
              keyspace: :string,
              type: :string

          "FUNCTION" ->
            unpack buffer,
              keyspace: :string,
              name: :string,
              args: :string_list

          "AGGREGATE" ->
            unpack buffer,
              keyspace: :string,
              name: :string,
              args: :string_list
        end

        Map.put(info, :options, options)
    end

    %__MODULE__{type: type, info: info}
  end
end
