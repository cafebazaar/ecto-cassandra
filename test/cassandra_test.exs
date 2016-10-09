defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Connection
  alias CQL.Supported
  alias CQL.Result.Prepared

  setup_all do
    {:ok, connection} = Connection.start_link(keyspace: "elixir_cql_test")
    Connection.query(connection, "drop keyspace elixir_cql_test;")
    Connection.query connection, """
      create keyspace elixir_cql_test
        with replication = {'class':'SimpleStrategy','replication_factor':1};
    """
    Connection.use(connection, "elixir_cql_test")
    Connection.query connection, """
      create table users (
        userid uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (userid)
      );
    """
    {:ok, %{connection: connection}}
  end

  setup %{connection: connection} do
    Connection.query(connection, "TRUNCATE users;")
    {:ok, %{connection: connection}}
  end

  test "OPTIONS", %{connection: connection} do
    assert {:ok, %Supported{options: options}} = Connection.options(connection)
    assert ["COMPRESSION", "CQL_VERSION"] = Keyword.keys(options)
  end

  test "REGISTER", %{connection: connection} do
    assert {:error, %CQL.Error{
      code: :protocol_error,
      message: "Invalid value 'BAD_TYPE' for Type",
    }} = Connection.register(connection, "BAD_TYPE")

    assert {:stream, stream} = Connection.register(connection, "SCHEMA_CHANGE")
    task = Task.async(Enum, :take, [stream, 1])
    Connection.query connection, """
      create table event_test (
        id uuid,
        PRIMARY KEY (id)
      );
    """
    assert [%CQL.Event{
      type: "SCHEMA_CHANGE",
      info: %{
        change: "CREATED",
        target: "TABLE",
        options: %{keyspace: "elixir_cql_test", table: "event_test"},
      },
    }] = Task.await(task)
  end

  test "INSERT", %{connection: connection} do
    assert :ok =
      Connection.query connection, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """
  end

  test "INSERT SELECT", %{connection: connection} do
    assert :ok = Connection.query connection, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, [%{"name" => "john doe"}]} = Connection.query(connection, "select name from users;")
  end

  test "PREPARE", %{connection: connection} do
    {:ok, %Prepared{id: id}} = Connection.prepare connection, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), ?, ?, ?, toTimestamp(now()));
    """

    assert :ok = Connection.execute(connection, id, %{name: "john doe", address: "UK", age: 27})

    assert {:ok, %Prepared{id: id}} = Connection.prepare(connection, "select name, age from users where age=? and address=? ALLOW FILTERING")

    assert {:ok, [%{"name" => "john doe"}]} = Connection.execute(connection, id, [27, "UK"])
  end

  test "DELETE", %{connection: connection} do
    assert :ok = Connection.query connection, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, data} = Connection.query(connection, "select * from users where name='john doe' limit 1 ALLOW FILTERING")
    user_id = data |> hd |> Map.get("userid")

    {:ok, %Prepared{id: id}} = Connection.prepare(connection, "delete from users where userid=?")
    assert :ok = Connection.execute(connection, id, [{:uuid, user_id}])
  end

  test "UPDATE", %{connection: connection} do
    assert :ok = Connection.query connection, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, data} = Connection.query(connection, "select * from users where name='john doe' limit 1 ALLOW FILTERING")
    user_id = data |> hd |> Map.get("userid")

    {:ok, %Prepared{id: id}} = Connection.prepare connection, """
      update users set age=?, address=? where userid=?
    """

    assert :ok = Connection.execute(connection, id, [27, "UK", {:uuid, user_id}])

    assert {:ok, %Prepared{id: id}} = Connection.prepare(connection, "select name,age from users where age=? and address=? ALLOW FILTERING")

    assert {:ok, [%{"name" => "john doe"}]} = Connection.execute(connection, id, [27, "UK"])
  end

  test "ERROR", %{connection: connection} do
    assert %CQL.Error{code: :invalid, message: "unconfigured table some_table"} =
      Connection.query(connection, "select * from some_table")
    assert %CQL.Error{code: :syntax_error, message: "line 1:5 mismatched character '<EOF>' expecting set null"} =
      Connection.query(connection, "-----")
  end
end
