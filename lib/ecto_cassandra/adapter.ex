defmodule EctoCassandra.Adapter do
  use EctoCassandra.Adapter.Base

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

  def execute(repo, %{fields: fields} = meta, query, params, process, options) do
    [cql | _] = args = super(repo, meta, query, params, process, options)
    Logger.debug(cql)
    case apply(&repo.execute/2, args) do
      {:ok, %{rows_count: count, rows: rows}} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}
      {:ok, :done} ->
        :ok
      {:error, {code, message}} ->
        Logger.debug("ERROR [#{code}] #{message}")
        raise RuntimeError, message: message
    end
  end

  def insert(repo, meta, fields, on_conflict, autogenerate, options) do
    args = super(repo, meta, fields, on_conflict, autogenerate, options)
    apply(&exec/5, args)
  end

  def insert_all(repo, meta, header, list, on_conflict, returning, options) do
    args = super(repo, meta, header, list, on_conflict, returning, options)
    apply(&exec/5, args)
  end

  def update(repo, meta, fields, filters, returning, options) do
    args = super(repo, meta, fields, filters, returning, options)
    apply(&exec/4, args)
  end

  def delete(repo, meta, filters, options) do
    args = super(repo, meta, filters, options)
    apply(&exec/4, args)
  end

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
