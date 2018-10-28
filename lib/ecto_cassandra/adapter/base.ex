defmodule EctoCassandra.Adapter.Base do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      def prepare(type, query) do
        cql = apply(EctoCassandra, type, [query])
        # {:cache, {:erlang.phash2(query), type, cql}}
        {:nocache, {query, type, cql}}
      end

      def execute(repo, _meta, {:cache, update, {hash, type, cql}}, params, _process, options) do
        update.({hash, type, cql})
        options = Keyword.put(options, :values, params)
        [cql, options]
      end

      def execute(repo, _meta, {:cached, _reset, {_hash, _type, cql}}, params, _process, options) do
        options = Keyword.put(options, :values, params)
        [cql, options]
      end

      def execute(repo, _meta, {:nocache, {_hash, _type, cql}}, params, _process, options) do
        options = Keyword.put(options, :values, params)
        [cql, options]
      end

      def insert(repo, %{source: {prefix, source}, schema: schema}, fields, on_conflict, autogenerate, options) do
        types = schema.__schema__(:types)
        {_field_names, values} = Enum.unzip(fields)
        {query_options, options} = Enum.split_with(options, fn {key, _} -> key in [:if, :using] end)
        cql = EctoCassandra.insert(prefix, source, fields, autogenerate, types, query_options)
        options = Keyword.put(options, :values, values)
        [repo, cql, options, on_conflict]
      end

      def insert_all(repo, %{source: {prefix, source}, schema: schema}, header, list, on_conflict, [], options) do
        autogenerate = {auto_column, _} = schema.__schema__(:autogenerate_id)
        header = header -- [auto_column]
        fields = Enum.zip(header, Stream.cycle([nil]))
        types = schema.__schema__(:types)
        cql = EctoCassandra.insert(prefix, source, fields, [autogenerate], types, options)
        [repo, {cql, list}, options, on_conflict]
      end

      def update(repo, %{source: {prefix, source}, schema: schema}, fields, filters, [], options) do
        types = schema.__schema__(:types)
        {_field_names, values} = Enum.unzip(fields)
        {filters, filter_values} = Enum.unzip(filters)
        {query_options, options} = Enum.split_with(options, fn {key, _} -> key in [:if, :using] end)
        cql = EctoCassandra.update(prefix, source, fields, filters, types, query_options)
        options = Keyword.put(options, :values, values ++ filter_values)
        [repo, cql, options]
      end

      def delete(repo, %{source: {prefix, source}}, filters, options) do
        {query_options, options} = Enum.split_with(options, fn {key, _} -> key in [:if, :using] end)
        {filters, filter_values} = Enum.unzip(filters)
        cql = EctoCassandra.delete(prefix, source, filters, query_options)
        options = Keyword.put(options, :values, filter_values)
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
