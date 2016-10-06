defmodule CQL.Error do
  import CQL.DataTypes.Decoder

  defstruct [:code, :message, :info]

  @codes %{
    0x0000 => :server_error,
    0x000A => :protocol_error,
    0x0100 => :authentication_error,
    0x1000 => :unavailable,
    0x1001 => :overloaded,
    0x1002 => :is_bootstrapping,
    0x1003 => :truncate_error,
    0x1100 => :write_timeout,
    0x1200 => :read_timeout,
    0x1300 => :read_failure,
    0x2000 => :syntax_error,
    0x2100 => :unauthorized,
    0x2200 => :invalid,
    0x2300 => :config_error,
    0x2400 => :already_exists,
    0x2500 => :unprepared,
  }

  def decode(buffer) do
    {error, rest} = unpack buffer,
      code:    :int,
      message: :string

    code = Map.get(@codes, error.code)

    {info, ""} = case code do
      :unavailable ->
        unpack rest,
          consistency: :consistency,
          required:    :int,
          alive:       :int

      :write_timeout ->
        unpack rest,
          consistency: :consistency,
          received:    :int,
          blockfor:    :int,
          write_type:  :string

      :write_failure ->
        unpack rest,
          consistency:  :consistency,
          received:     :int,
          blockfor:     :int,
          num_failures: :int,
          write_type:   :string

      :read_timeout ->
        unpack rest,
          consistency:  :consistency,
          received:     :int,
          blockfor:     :int,
          data_present: :boolean

      :read_failure ->
        unpack rest,
          consistency:  :consistency,
          received:     :int,
          blockfor:     :int,
          num_failures: :int,
          data_present: :boolean

      :function_failure ->
        unpack rest,
          keyspace:  :string,
          function:  :string,
          arg_types: :string_list

      :already_exists ->
        unpack rest,
          keyspace: :string,
          table:    :string

      :unprepared ->
        unpack rest,
          id: :short_bytes

      _any_other ->
        {rest, ""}
    end

    %__MODULE__{code: code, message: error.message, info: info}
  end
end
