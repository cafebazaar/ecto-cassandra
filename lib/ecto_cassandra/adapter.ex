defmodule EctoCassandra.Adapter do
  @moduledoc """
  Ecto Adapter for Apache Cassandra.

  It uses `cassandra` for communicating to the database
  """

  use EctoCassandra.Adapter.Base

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Storage

  @host_tries 3

  ### Ecto.Adapter.Migration Callbacks ###

  @doc false
  def execute_ddl(repo, definitions, options) do
    options = Keyword.put(options, :on_coordinator, true)
    cql = EctoCassandra.ddl(definitions)

    case exec_and_log(repo, cql, options) do
      %CQL.Result.SchemaChange{} -> :ok
      %CQL.Result.Void{}         -> :ok
      error                      -> raise error
    end
  end

  @doc false
  def supports_ddl_transaction?, do: false

  ### Ecto.Adapter.Storage Callbacks ###

  @doc false
  def storage_up(options) do
    options = Keyword.put(options, :on_coordinator, true)

    cql =
      options
      |> Keyword.put(:if_not_exists, true)
      |> EctoCassandra.create_keyspace

    case run_query(cql, options) do
      %CQL.Result.SchemaChange{change_type: "CREATED", target: "KEYSPACE"} ->
        :ok
      %CQL.Result.Void{} ->
        {:error, :already_up}
      error ->
        {:error, Exception.message(error)}
    end
  end

  @doc false
  def storage_down(options) do
    options = Keyword.put(options, :on_coordinator, true)

    cql =
      options
      |> Keyword.put(:if_exists, true)
      |> EctoCassandra.drop_keyspace

    case run_query(cql, options) do
      %CQL.Result.SchemaChange{change_type: "DROPPED", target: "KEYSPACE"} ->
        :ok
      %CQL.Result.Void{} ->
        {:error, :already_down}
      error ->
        {:error, Exception.message(error)}
    end
  end

  ### Ecto.Adapter Callbacks ###

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      defmodule CassandraRepo do
        use Cassandra
      end

      defdelegate execute(statement, options), to: CassandraRepo

      def __cassandra_repo__, do: CassandraRepo
    end
  end

  @doc false
  def child_spec(repo, options) do
    import Supervisor.Spec
    supervisor(repo.__cassandra_repo__, [options])
  end

  @doc false
  def ensure_all_started(_repo, _type) do
    Application.ensure_all_started(:cassandra)
  end

  @doc false
  def execute(repo, %{fields: fields} = meta, query, params, process, options) do
    [cql, options] = super(repo, meta, query, params, process, options)

    case exec_and_log(repo, cql, options) do
      %CQL.Result.Rows{rows_count: count, rows: rows} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}
      %CQL.Result.Void{} -> :ok
      error              -> raise error
    end
  end

  @doc false
  def insert(repo, meta, fields, on_conflict, autogenerate, options) do
    args = super(repo, meta, fields, on_conflict, autogenerate, options)
    apply(&exec/4, args)
  end

  @doc false
  def insert_all(repo, meta, header, list, on_conflict, returning, options) do
    args = super(repo, meta, header, list, on_conflict, returning, options)
    apply(&exec/4, args)
  end

  @doc false
  def update(repo, meta, fields, filters, returning, options) do
    args = super(repo, meta, fields, filters, returning, options)
    apply(&exec/3, args)
  end

  @doc false
  def delete(repo, meta, filters, options) do
    args = super(repo, meta, filters, options)
    apply(&exec/3, args)
  end

  ### Helpers ###

  defp run_query(cql, options) do
    options
    |> Keyword.get(:contact_points, [])
    |> List.duplicate(@host_tries)
    |> List.flatten
    |> Stream.map(&Cassandra.Connection.run_query(&1, cql, options))
    |> Stream.reject(&match?(%Cassandra.ConnectionError{}, &1))
    |> Enum.take(1)
    |> case do
      [result] -> result
      []       -> raise RuntimeError, "connections refused"
    end
  end

  defp exec(repo, cql, options, on_conflict \\ :error) do
    case exec_and_log(repo, cql, options) do
      %CQL.Result.Void{} ->
        {:ok, []}
      %CQL.Result.Rows{rows_count: 1, rows: [[true | _]], columns: ["[applied]"|_]} ->
        {:ok, []}
      %CQL.Result.Rows{rows_count: 1, rows: [[false | _]], columns: ["[applied]"|_]} ->
        if on_conflict == :nothing do
          {:ok, []}
        else
          {:error, :stale}
        end
      error -> raise error
    end
  end

  defp exec_and_log(repo, cql, options) do
    if Keyword.get(options, :log, true) do
      repo.execute(cql, Keyword.put(options, :log, &log(repo, cql, &1)))
    else
      repo.execute(cql, Keyword.delete(options, :log))
    end
  end

  defp log(repo, cql, entry) do
    %{connection_time: query_time,
      decode_time: decode_time,
      pool_time: queue_time,
      result: result,
      query: query,
    } = entry

    repo.__log__(%Ecto.LogEntry{
      query_time: query_time,
      decode_time: decode_time,
      queue_time: queue_time,
      result: log_result(result),
      params: Map.get(query, :values, []),
      query: String.Chars.to_string(cql),
      ansi_color: cql_color(cql),
    })
  end

  defp log_result({:ok, _query, res}), do: {:ok, res}
  defp log_result(other), do: other

  defp cql_color("SELECT" <> _), do: :cyan
  defp cql_color("INSERT" <> _), do: :green
  defp cql_color("UPDATE" <> _), do: :yellow
  defp cql_color("DELETE" <> _), do: :red
  defp cql_color("TRUNC" <> _), do: :red
  defp cql_color(_), do: nil
end
