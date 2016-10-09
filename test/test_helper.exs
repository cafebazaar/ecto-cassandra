ExUnit.start
ExCheck.start

alias Cassandra.Connection

{:ok, connection} = Connection.start_link
{:ok, _} = Connection.query connection, "DROP KEYSPACE IF EXISTS elixir_cassandra_test;"
{:ok, _} = Connection.query connection, """
  CREATE KEYSPACE elixir_cassandra_test
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  """

System.at_exit fn _ ->
  {:ok, connection} = Connection.start_link
  {:ok, _} = Connection.query connection, "DROP KEYSPACE IF EXISTS elixir_cassandra_test;"
end
