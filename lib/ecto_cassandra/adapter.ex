defmodule EctoCassandra.Adapter do
  require Logger

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Storage

  ### Ecto.Adapter.Migration Callbacks ###

  def execute_ddl(repo, definitions, options) do
    cql = EctoCassandra.ddl(definitions)
    options = Keyword.put_new(options, :consistency, :all)
    Logger.debug(cql)
    case repo.execute(cql, options) do
      {:ok, result} ->
        Logger.debug(inspect result)
        :ok
      {:error, {code, message}} ->
        Logger.debug("ERROR [#{code}] #{message}")
        raise RuntimeError, message: message
    end
  end

  def supports_ddl_transaction?, do: false

  ### Ecto.Adapter.Storage Callbacks ###

  def storage_up(options) do
    cql =
      options
      |> Keyword.put(:if_not_exists, true)
      |> EctoCassandra.create_keyspace

    case run_query(cql, options) do
      {:ok, %CQL.Result.SchemaChange{change_type: "CREATED", target: "KEYSPACE"}} ->
        :ok
      {:ok, :done} ->
        {:error, :already_up}
      {:error, {_code, error}} ->
        {:error, error}
    end
  end

  def storage_down(options) do
    cql =
      options
      |> Keyword.put(:if_exists, true)
      |> EctoCassandra.drop_keyspace

    case run_query(cql, options) do
      {:ok, %CQL.Result.SchemaChange{change_type: "DROPPED", target: "KEYSPACE"}} ->
        :ok
      {:ok, :done} ->
        {:error, :already_down}
      {:error, {_code, error}} ->
        {:error, error}
    end
  end

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

  def ensure_all_started(_repo, _type) do
    {:ok, []}
  end

  def prepare(type, query) do
    {:nocache, {type, query}}
  end

  def execute(repo, %{fields: fields}, {:nocache, {type, query}}, params, process, options) do
    {cql, values, options} = apply(EctoCassandra, type, [query, options])
    options = Keyword.put(options, :values, values ++ params)
    Logger.debug(cql)

    case repo.execute(cql, options) do
      {:ok, %{rows_count: count, rows: rows}} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}
      {:ok, :done} ->
        :ok
      {:error, {code, message}} ->
        Logger.debug("ERROR [#{code}] #{message}")
        raise RuntimeError, message: message
    end
  end

  def insert(repo, %{source: {prefix, source}, schema: schema}, fields, on_conflict, autogenerate, options) do
    autogenerate = Enum.map(autogenerate, &{&1, schema.__schema__(:type, &1)})
    {cql, values, options} = EctoCassandra.insert(prefix, source, fields, autogenerate, options)
    exec(repo, cql, values, options, on_conflict)
  end

  def insert_all(repo, %{source: {prefix, source}, schema: schema}, header, list, on_conflict, [], options) do
    autogenerate = {auto_column, _} = schema.__schema__(:autogenerate_id)
    header = header -- [auto_column]
    fields = Enum.zip(header, Stream.cycle([nil]))
    {cql, values, options} = EctoCassandra.insert(prefix, source, fields, [autogenerate], options)
    exec(repo, {cql, list}, values, options, on_conflict)
  end

  def update(repo, %{source: {prefix, source}}, fields, filters, [], options) do
    {cql, values, options} = EctoCassandra.update(prefix, source, fields, filters, options)
    exec(repo, cql, values, options)
  end

  def delete(repo, %{source: {prefix, source}}, filters, options) do
    {cql, values, options} = EctoCassandra.delete(prefix, source, filters, options)
    exec(repo, cql, values, options)
  end

  def autogenerate(:id), do: nil
  def autogenerate(:binary_id), do: nil

  def dumpers(:binary_id, type), do: [type]
  def dumpers(:naive_datetime, _type), do: [&is_naive/1]
  def dumpers(_primitive, type), do: [type]

  def loaders(:binary_id, type), do: [&load_uuid/1, type]
  def loaders(:naive_datetime, _type), do: [&is_naive/1]
  def loaders(_primitive, type), do: [type]

  def transaction(_repo, _options, _func) do
    {:error, :not_supported}
  end

  def in_transaction?(_repo), do: false

  def rollback(_repo, _value), do: nil

  ### Helpers ###

  defp run_query(cql, options) do
    Logger.debug("Executing `#{cql}`")
    options = Keyword.put(options, :keyspace, nil)
    {:ok, cluster} = Cassandra.Cluster.start_link(options[:contact_points], options)
    {:ok, session} = Cassandra.Session.start_link(cluster, options)
    result = Cassandra.Session.execute(session, cql, options)
    :ok = GenServer.stop(cluster)
    :ok = GenServer.stop(session)
    result
  end

  defp exec(repo, cql, values, options, on_conflict \\ :error) do
    Logger.debug("Executing `#{inspect cql}` with values: #{inspect values}")
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
      {code, message} ->
        Logger.debug("ERROR [#{code}] #{message}")
        raise RuntimeError, message: message
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

  defp load_uuid(%Cassandra.UUID{value: value}), do: {:ok, value}
  defp load_uuid(value), do: {:ok, value}

  defp is_naive(%NaiveDateTime{} = datetime) do
    {:ok, datetime}
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
