defmodule EctoCassandra.Query do

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
      import EctoCassandra.Query
    end
  end

  defmacro token(fields) when is_list(fields) do
    marks = Enum.map_join(fields, ", ", fn _ -> "?" end)
    quote do: fragment(unquote("token(#{marks})"), unquote_splicing(fields))
  end

  defmacro token(field) do
    quote do: fragment("token(?)", unquote(field))
  end

  defmacro cast(field, type) when type in @types do
    fragment = "cast(? as #{Atom.to_string(type)})"
    quote do: fragment(unquote(fragment), unquote(field))
  end

  defmacro uuid do
    quote do: fragment("uuid()")
  end

  defmacro now do
    quote do: fragment("now()")
  end

  defmacro min_timeuuid(time) do
    quote do: fragment("minTimeuuid(?)", unquote(time))
  end

  defmacro max_timeuuid(time) do
    quote do: fragment("maxTimeuuid(?)", unquote(time))
  end

  defmacro to_date(time) do
    quote do: fragment("toDate(?)", unquote(time))
  end

  defmacro to_timestamp(time) do
    quote do: fragment("toTimestamp(?)", unquote(time))
  end

  defmacro to_unix_timestamp(time) do
    quote do: fragment("toUnixTimestamp(?)", unquote(time))
  end

  defmacro as_blob(field, type) when type in @types do
    fragment = "#{Atom.to_string(type)}AsBlob(?)"
    quote do: fragment(unquote(fragment), unquote(field))
  end

  defmacro contains(field, value) do
    quote do: fragment("? CONTAINS ?", unquote(field), unquote(value))
  end

  @doc """
  Be aware that batches are often mistakenly used in an attempt to optimize performance.

  Refere to http://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html
  """
  defmacro batch(repo, options \\ [], [do: {:__block__, _, statements}]) do
    quote do: EctoCassandra.Batch.batch(unquote(repo), unquote(options), unquote(statements))
  end

  defmacro insert(struct, options \\ []) do
    quote do: {:insert, unquote(struct), unquote(options)}
  end

  defmacro update(struct, options \\ []) do
    quote do: {:update, unquote(struct), unquote(options)}
  end

  defmacro delete(struct, options \\ []) do
    quote do: {:delete, unquote(struct), unquote(options)}
  end
end
