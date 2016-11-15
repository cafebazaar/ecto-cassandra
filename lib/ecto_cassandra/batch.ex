defmodule EctoCassandra.Batch do
  @moduledoc false

  require Logger

  use EctoCassandra.Adapter.Base

  @adapter EctoCassandra.Batch

  def insert(struct, opts \\ []) do
    Ecto.Repo.Schema.insert(__MODULE__, @adapter, struct, opts)
  end

  def update(struct, opts \\ []) do
    Ecto.Repo.Schema.update(__MODULE__, @adapter, struct, opts)
  end

  def insert_or_update(changeset, opts \\ []) do
    Ecto.Repo.Schema.insert_or_update(__MODULE__, @adapter, changeset, opts)
  end

  def delete(struct, opts \\ []) do
    Ecto.Repo.Schema.delete(__MODULE__, @adapter, struct, opts)
  end

  def insert(repo, meta, fields, on_conflict, autogenerate, options) do
    args = super(repo, meta, fields, on_conflict, autogenerate, options)
    apply(&add_query/2, [options[:batch_agent], args])
  end

  def update(repo, meta, fields, filters, returning, options) do
    args = super(repo, meta, fields, filters, returning, options)
    apply(&add_query/2, [options[:batch_agent], args])
  end

  def delete(repo, meta, filters, options) do
    args = super(repo, meta, filters, options)
    apply(&add_query/2, [options[:batch_agent], args])
  end

  def execute(_repo, _meta, _query, _params, _process, _options) do
    error!
  end

  def insert_all(_repo, _meta, _header, _list, _on_conflict, _returning, _options) do
    error!
  end

  def batch(repo, options, statements) do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    Enum.each statements, fn
      {action, struct, options} ->
        apply(__MODULE__, action, [struct, Keyword.put(options, :batch_agent, agent)])
      {action, struct} ->
        apply(EctoCassandra.Batch, action, [struct, [batch_agent: agent]])
    end
    {queries, values} = get_queries(agent)
    {cql, cql_values, options} = EctoCassandra.batch(queries, options)
    values = cql_values ++ values
    Logger.debug("Executing:\n\n  #{cql}\n  #{inspect options}")
    exec(repo, cql, Keyword.put(options, :values, values))
  end

  defp exec(repo, cql, options) do
    case repo.execute(cql, options) do
      {:ok, :done} ->
        :ok
      {:ok, %{rows_count: 1, rows: [[true | _]], columns: ["[applied]"|_]}} ->
        :ok
      {:ok, %{rows_count: 1, rows: [[false | _]], columns: ["[applied]"|_]}} ->
        if options[:on_conflict] == :error do
          {:error, :stale}
        else
          {:ok, []}
        end
      {code, message} ->
        Logger.debug("ERROR [#{code}] #{message}")
        raise RuntimeError, message: message
    end
  end

  defp get_queries(batch_agent) do
    result = Agent.get batch_agent, fn queries ->
      Enum.reduce queries, {[], []}, fn({cql, values}, {acc_cqls, acc_values}) ->
        {[cql | acc_cqls], values ++ acc_values}
      end
    end
    Agent.stop(batch_agent)
    result
  end

  defp add_query(nil, _) do
    raise ArgumentError, "cann't batch without agent"
  end

  defp add_query(batch_agent, [_, cql, values | _]) do
    Agent.update batch_agent, fn queries ->
      [{cql, values} | queries]
    end
    {:ok, []}
  end

  defp error! do
    raise ArgumentError, "Cassandra batch can only include :insert, :delete and :update operations"
  end
end
