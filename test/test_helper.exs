ExUnit.start
ExCheck.start

defmodule Cassandra.ConnectionTest do
  def start_link do
    Cassandra.Connection.start_link(name: Cassandra.ConnectionTest, async_init: false)
  end

  def send(frame) do
    Cassandra.Connection.send(Cassandra.ConnectionTest, CQL.encode(frame))
  end

  def query(string) do
    __MODULE__.send(%CQL.Query{query: string})
  end

  def prepare(string) do
    __MODULE__.send(%CQL.Prepare{query: string})
  end

  def execute(prepared, values) do
    __MODULE__.send(%CQL.Execute{
      prepared: prepared,
      params: %CQL.QueryParams{values: values},
    })
  end
end

alias Cassandra.ConnectionTest, as: Conn

{:ok, _} = Conn.start_link
{:ok, _} = Conn.query "DROP KEYSPACE IF EXISTS elixir_cassandra_test;"
{:ok, _} = Conn.query """
  CREATE KEYSPACE elixir_cassandra_test
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};
"""

System.at_exit fn _ ->
  {:ok, _} = Conn.query "DROP KEYSPACE IF EXISTS elixir_cassandra_test;"
end
