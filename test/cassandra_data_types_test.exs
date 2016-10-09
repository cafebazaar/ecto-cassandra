defmodule CassandraDataTypesTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Connection
  alias CQL.Supported
  alias CQL.Result.Void

  setup_all do
    {:ok, connection} = Connection.start_link([])
    Connection.query(connection, "drop keyspace elixir_cql_test;")
    Connection.query connection, """
      create keyspace elixir_cql_test
        with replication = {'class':'SimpleStrategy','replication_factor':1};
    """
    Connection.query(connection, "USE elixir_cql_test;")
    Connection.query connection, """
      create table data_types (
        f_ascii     ascii,
        f_bigint    bigint,
        f_blob      blob,
        f_boolean   boolean,
        f_counter   counter,
        f_date      date,
        f_decimal   decimal,
        f_double    double,
        f_float     float,
        f_inet      inet,
        f_int       int,
        f_smallint  smallint,
        f_text      text,
        f_time      time,
        f_timestamp timestamp,
        f_timeuuid  timeuuid,
        f_tinyint   tinyint,
        f_uuid      uuid,
        f_varchar   varchar,
        f_varint    varint,
        f_map1      map<text, text>,
        f_map2      map<int, boolean>,
        f_list1     list<text>,
        f_list2     list<int>,
        f_set       set<text>
        PRIMARY KEY (f_timeuuid, f_timestamp, f_uuid)
      );
    """
    {:ok, %{connection: connection}}
  end

  setup %{connection: connection} do
    Connection.query(connection, "TRUNCATE data_types;")
    {:ok, %{connection: connection}}
  end
end
