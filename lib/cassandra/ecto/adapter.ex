defmodule Cassandra.Ecto.Adapter do
  require Logger

  ### Ecto.Adapter Callbacks ###

  @behaviour Ecto.Adapter

  defmacro __before_compile__(env) do
    config = Module.get_attribute(env.module, :config)
    keyspace = Keyword.fetch!(config, :keyspace)

    quote do
      defmodule Supervisor do
        use Cassandra
      end

      defdelegate execute(statement, options), to: Supervisor

      def __supervisor__, do: Supervisor
      def __keyspace__, do: unquote(keyspace)
    end
  end

  def child_spec(repo, options) do
    import Supervisor.Spec
    supervisor(repo.__supervisor__, [options])
  end

  def prepare(type, query) do
    {:nocache, {type, query}}
  end

  def execute(repo, %{fields: fields}, {:nocache, {type, query}}, params, process, options) do
    {cql, values, options} = apply(Cassandra.Ecto, type, [query, options])
    options = Keyword.put(options, :values, values ++ params)
    Logger.debug(cql)

    case repo.execute(cql, options) do
      {:ok, %{rows_count: count, rows: rows}} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}
      {:ok, :done} ->
        :ok
      error ->
        throw error
    end
  end

  def insert(repo, %{source: {prefix, source}}, fields, on_conflict, [], options) do
    {cql, values, options} = Cassandra.Ecto.insert(prefix, source, fields, options)
    exec(repo, cql, values, options, on_conflict)
  end

  def update(repo, %{source: {prefix, source}}, fields, filters, [], options) do
    {cql, values, options} = Cassandra.Ecto.update(prefix, source, fields, filters, options)
    exec(repo, cql, values, options, :error)
  end

  def delete(repo, %{source: {prefix, source}}, filters, options) do
    {cql, values, options} = Cassandra.Ecto.delete(prefix, source, filters, options)
    exec(repo, cql, values, options, :error)
  end

  def autogenerate(:id), do: %Cassandra.UUID{type: :uuid}
  def autogenerate(:binary_id), do: %Cassandra.UUID{type: :timeuuid}

  def dumpers(:binary_id, type), do: [type]
  def dumpers(_primitive, type), do: [type]

  def loaders(:binary_id, type), do: [&load_uuid/1, type]
  def loaders(_primitive, type), do: [type]

  def in_transaction?(_repo), do: false

  def rollback(_repo, _value), do: nil

  ### Helpers ###

  defp exec(repo, cql, values, options, on_conflict \\ :nothing) do
    Logger.debug("Executing `#{cql}` with values: #{inspect values}")
    case repo.execute(cql, Keyword.put(options, :values, values)) do
      {:ok, :done} ->
        {:ok, []}
      {:ok, %{rows_count: 1, rows: [[true | _]], columns: ["[applied]"|_]}} ->
        {:ok, []}
      {:ok, %{rows_count: 1, rows: [[false | _]], columns: ["[applied]"|_]}} ->
        if on_conflict == :nothing do
          {:ok, []}
        else
          {:error, :stale}
        end
      error ->
        throw error
    end
  end

  defp process_row(row, [{{:., _, _}, _, _} | _] = fields, process) do
    fields
    |> Enum.zip(row)
    |> Enum.map(fn {field, term} -> process.(field, term, nil) end)
  end

  defp process_row(row, fields, process) do
    Enum.map(fields, &process.(&1, row, nil))
  end

  defp put_if_nil(map, key, value) do
    Map.update map, key, value, fn
      nil   -> value
      other -> other
    end
  end

  defp load_uuid(%Cassandra.UUID{value: value}), do: {:ok, value}
  defp load_uuid(value), do: {:ok, value}
end

defmodule Repo do
  use Ecto.Repo, otp_app: :cassandra
end

defmodule User do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "users" do
    field :name, :string
    field :age,  :integer
  end
end
