defmodule EctoCassandra.Adapter.Base do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      def prepare(type, query) do
        {:cache, {:erlang.phash2(query), type, query}}
      end

      def execute(repo, %{fields: fields}, {:cache, update, {hash, type, query}}, params, process, options) do
        {cql, values, options} = apply(EctoCassandra, type, [query, options])
        update.({hash, cql})
        options = Keyword.put(options, :values, params ++ values)
        [cql, options]
      end

      def execute(repo, _meta, {:cached, _reset, {_hash, cql}}, params, _process, options) do
        options = Keyword.put(options, :values, params)
        [cql, options]
      end

      def execute(repo, %{fields: fields}, {:nocache, {_hash, type, query}}, params, process, options) do
        {cql, options} = apply(EctoCassandra, type, [query, options])
        options = Keyword.put(options, :values, params)
        [cql, options]
      end

      def insert(repo, %{source: {prefix, source}, schema: schema}, fields, on_conflict, autogenerate, options) do
        types = schema.__schema__(:types)
        {cql, options} = EctoCassandra.insert(prefix, source, fields, autogenerate, types, options)
        [repo, cql, options, on_conflict]
      end

      def insert_all(repo, %{source: {prefix, source}, schema: schema}, header, list, on_conflict, [], options) do
        autogenerate = {auto_column, _} = schema.__schema__(:autogenerate_id)
        header = header -- [auto_column]
        fields = Enum.zip(header, Stream.cycle([nil]))
        types = schema.__schema__(:types)
        {cql, options} = EctoCassandra.insert(prefix, source, fields, [autogenerate], types, options)
        [repo, {cql, list}, options, on_conflict]
      end

      def update(repo, %{source: {prefix, source}, schema: schema}, fields, filters, [], options) do
        types = schema.__schema__(:types)
        {cql, options} = EctoCassandra.update(prefix, source, fields, filters, types, options)
        [repo, cql, options]
      end

      def delete(repo, %{source: {prefix, source}}, filters, options) do
        {cql, options} = EctoCassandra.delete(prefix, source, filters, options)
        [repo, cql, options]
      end

      def autogenerate(_), do: nil

      def dumpers(:utc_datetime, _type), do: [&to_naive/1]
      def dumpers(:naive_datetime, _type), do: [&to_naive/1]
      def dumpers(_primitive, type), do: [type]

      def loaders(:binary_id, type), do: [&load_uuid/1, type]
      def loaders(:utc_datetime, _type), do: [&to_datetime/1]
      def loaders(:naive_datetime, _type), do: [&to_naive/1]
      def loaders(_primitive, type), do: [type]

      def transaction(_repo, _options, func) do
        case func.() do
          {:error, _} = error ->
            error
          value ->
            {:ok, value}
        end
      end

      def in_transaction?(_repo), do: false

      def rollback(_repo, _value), do: nil

      defp process_row(row, [{:&, _, _} | _] = fields, process) do
        Enum.map(fields, &process.(&1, row, nil))
      end

      defp process_row(row, fields, process) do
        fields
        |> Enum.zip(row)
        |> Enum.map(fn {field, term} -> process.(field, term, nil) end)
      end

      defp load_uuid(%Cassandra.UUID{value: value}), do: {:ok, value}
      defp load_uuid(value), do: {:ok, value}

      defp to_naive(%NaiveDateTime{} = datetime), do: {:ok, datetime}
      defp to_naive(%DateTime{} = datetime), do: {:ok, DateTime.to_naive(datetime)}
      defp to_naive(_), do: :error

      defp to_datetime(%NaiveDateTime{} = naive) do
        values =
          naive
          |> Map.from_struct
          |> Map.merge(%{std_offset: 0, time_zone: "Etc/UTC", utc_offset: 0, zone_abbr: "UTC"})

        {:ok, struct(DateTime, values)}
      end
      defp to_datetime(%DateTime{} = datetime), do: {:ok, datetime}
      defp to_datetime(_), do: :error

      defoverridable [
        autogenerate: 1,
        delete: 4,
        execute: 6,
        insert: 6,
        insert_all: 7,
        update: 6,
      ]
    end
  end
end
