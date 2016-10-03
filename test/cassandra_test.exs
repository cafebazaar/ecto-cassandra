defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  import Cassandra.Protocol, only: [run: 2, connect: 1]

  alias CQL.{Options, Supported, Query, Prepare, Execute, QueryParams}
  alias CQL.Result.{Void, Rows}

  setup_all do
    {:ok, %{socket: socket}} = connect([])
    run socket, "drop keyspace elixir_cql_test;"
    run socket, """
      create keyspace elixir_cql_test
        with replication = {'class':'SimpleStrategy','replication_factor':1};
    """
    run socket, "USE elixir_cql_test;"
    run socket, """
      create table users (
        userid uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (userid, joined_at)
      );
    """
    :ok
  end

  setup do
    {:ok, %{socket: socket}} = connect([])
    run socket, "USE elixir_cql_test;"
    {:ok, %{socket: socket}}
  end

  test "OPTIONS", %{socket: socket} do
    assert %Supported{options: options} = run socket, %Options{}
    assert ["CQL_VERSION", "COMPRESSION"] = Keyword.keys(options)
  end

  test "INSERT", %{socket: socket} do
    assert %Void{} = run socket, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'fred smith', 20, 'US', toTimestamp(now()));
    """
  end

  test "INSERT SELECT", %{socket: socket} do
    assert %Void{} = run socket, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    rows = run socket, "select * from users;"
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end
end
