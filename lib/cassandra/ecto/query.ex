defmodule Cassandra.Ecto.Query do

  @types [
    :ascii,
    :bigint,
    :blob,
    :boolean,
    :counter,
    :date,
    :decimal,
    :double,
    :float,
    :inet,
    :int,
    :smallint,
    :text,
    :time,
    :timestamp,
    :timeuuid,
    :tinyint,
    :uuid,
    :varchar,
    :varint,
  ]

  defmacro __using__([]) do
    quote do
      import Ecto.Query
      import Cassandra.Ecto.Query
    end
  end

  defmacro token(field) do
    quote do
      fragment("token(?)", unquote(field))
    end
  end

  defmacro cast(field, type) when type in @types do
    fragment = "cast(? as #{Atom.to_string(type)})"
    quote do
      fragment(fragment, unquote(field))
    end
  end

  defmacro uuid do
    quote do
      fragment("uuid()")
    end
  end

  defmacro now do
    quote do
      fragment("now()")
    end
  end

  defmacro min_timeuuid(time) do
    quote do
      fragment("minTimeuuid(?)", unquote(time))
    end
  end

  defmacro max_timeuuid(time) do
    quote do
      fragment("maxTimeuuid(?)", unquote(time))
    end
  end

  defmacro to_date(time) do
    quote do
      fragment("toDate(?)", unquote(time))
    end
  end

  defmacro to_timestamp(time) do
    quote do
      fragment("toTimestamp(?)", unquote(time))
    end
  end

  defmacro to_unix_timestamp(time) do
    quote do
      fragment("toUnixTimestamp(?)", unquote(time))
    end
  end

  defmacro as_blob(field, type) when type in @types do
    fragment = "#{Atom.to_string(type)}AsBlob(?)"
    quote do
      fragment(unquote(fragment), unquote(field))
    end
  end
end
