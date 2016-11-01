ExUnit.start
ExCheck.start

alias Cassandra.Connection

defmodule Cassandra.TestHelper do
  @keyspace "elixir_cassandra_test"

  def keyspace, do: @keyspace

  def host do
    System.get_env("CASSANDRA_SEED") || "127.0.0.1"
  end

  def drop_keyspace do
    %CQL.Query{query: "DROP KEYSPACE IF EXISTS #{@keyspace};"}
  end

  def create_keyspace do
    %CQL.Query{query: """
      CREATE KEYSPACE #{@keyspace}
        WITH replication = {
          'class': 'SimpleStrategy',
          'replication_factor': 1
        };
      """
    }
  end

  def setup do
    {:ok, c} = Connection.start_link(host: host, async_init: false)
    {:ok, _} = Connection.send(c, drop_keyspace)
    {:ok, _} = Connection.send(c, create_keyspace)

    Connection.stop(c)
  end

  def teardown do
    {:ok, c} = Connection.start_link(host: host, async_init: false)
    {:ok, _} = Connection.send(c, drop_keyspace)

    Connection.stop(c)
  end
end

System.at_exit fn _ ->
  Cassandra.TestHelper.teardown
end

Cassandra.TestHelper.setup

