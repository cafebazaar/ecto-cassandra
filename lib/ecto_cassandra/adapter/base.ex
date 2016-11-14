defmodule EctoCassandra.Adapter.Base do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      def prepare(type, query) do
        {:nocache, {type, query}}
      end

      def execute(repo, %{fields: fields}, {:nocache, {type, query}}, params, process, options) do
        {cql, values, options} = apply(EctoCassandra, type, [query, options])
        options = Keyword.put(options, :values, values ++ params)
        [cql, options]
      end

      def insert(repo, %{source: {prefix, source}, schema: schema}, fields, on_conflict, autogenerate, options) do
        autogenerate = Enum.map(autogenerate, &{&1, schema.__schema__(:type, &1)})
        {cql, values, options} = EctoCassandra.insert(prefix, source, fields, autogenerate, options)
        [repo, cql, values, options, on_conflict]
      end

      def insert_all(repo, %{source: {prefix, source}, schema: schema}, header, list, on_conflict, [], options) do
        autogenerate = {auto_column, _} = schema.__schema__(:autogenerate_id)
        header = header -- [auto_column]
        fields = Enum.zip(header, Stream.cycle([nil]))
        {cql, values, options} = EctoCassandra.insert(prefix, source, fields, [autogenerate], options)
        [repo, {cql, list}, values, options, on_conflict]
      end

      def update(repo, %{source: {prefix, source}}, fields, filters, [], options) do
        {cql, values, options} = EctoCassandra.update(prefix, source, fields, filters, options)
        [repo, cql, values, options]
      end

      def delete(repo, %{source: {prefix, source}}, filters, options) do
        {cql, values, options} = EctoCassandra.delete(prefix, source, filters, options)
        [repo, cql, values, options]
      end

      def autogenerate(_), do: nil

      def dumpers(:naive_datetime, _type), do: [&is_naive/1]
      def dumpers(_primitive, type), do: [type]

      def loaders(:binary_id, type), do: [&load_uuid/1, type]
      def loaders(:naive_datetime, _type), do: [&is_naive/1]
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

      defp process_row(row, [{{:., _, _}, _, _} | _] = fields, process) do
        fields
        |> Enum.zip(row)
        |> Enum.map(fn {field, term} -> process.(field, term, nil) end)
      end

      defp process_row(row, fields, process) do
        Enum.map(fields, &process.(&1, row, nil))
      end

      defp load_uuid(%Cassandra.UUID{value: value}), do: {:ok, value}
      defp load_uuid(value), do: {:ok, value}

      defp is_naive(%NaiveDateTime{} = datetime) do
        {:ok, datetime}
      end

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
