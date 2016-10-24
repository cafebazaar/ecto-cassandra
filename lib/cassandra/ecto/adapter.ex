defmodule Cassandra.Ecto.Adapter do
  @behaviour Ecto.Adapter

  require Logger

  ### Ecto.Adapter Callbacks ###

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

  def prepare(_atom, query) do
    {:nocache, query}
  end

  def execute(repo, %{fields: fields} = meta, {:nocache, query}, params, process, options) do
    cql =
      query
      |> put_if_nil(:prefix, repo.__keyspace__)
      |> Cassandra.Ecto.to_cql

    Logger.debug(cql)

    case repo.execute(cql, Keyword.put(options, :values, params)) do
      {:ok, %{rows_count: count, rows: rows}} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}
      error ->
        throw error
    end
  end

  def dumpers(:binary_id, type), do: [type]
  def dumpers(_primitive, type), do: [type]

  def loaders(:binary_id, type), do: [type]
  def loaders(_primitive, type), do: [type]

  ### Helpers ###

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
