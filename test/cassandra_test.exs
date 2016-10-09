defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Connection
  alias CQL.Supported
  alias CQL.Result.Prepared

  setup_all do
    {:ok, connection} = Connection.start_link(keyspace: "elixir_cql_test")
    {:ok, _} = Connection.query connection, """
      CREATE TABLE users (
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
    {:ok, :done} = Connection.query(connection, "TRUNCATE users;")
    {:ok, %{connection: connection}}
  end

  test "OPTIONS", %{connection: connection} do
    assert {:ok, %Supported{options: options}} = Connection.options(connection)
    assert ["COMPRESSION", "CQL_VERSION"] = Keyword.keys(options)
  end

  test "REGISTER", %{connection: connection} do
    assert {:error, {:protocol_error, "Invalid value 'BAD_TYPE' for Type"}} =
      Connection.register(connection, "BAD_TYPE")

    assert {:stream, stream} = Connection.register(connection, "SCHEMA_CHANGE")
    task = Task.async(Enum, :take, [stream, 1])
    Connection.query connection, """
      CREATE TABLE event_test (
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
    assert {:ok, :done} =
      Connection.query connection, """
      INSERT INTO users (userid, name, age, address, joined_at)
        VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """
  end

  test "INSERT SELECT", %{connection: connection} do
    assert {:ok, :done} = Connection.query connection, """
      INSERT INTO users (userid, name, age, address, joined_at)
        VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, [%{"name" => "john doe"}]} = Connection.query(connection, "SELECT name FROM users;")
  end

  test "PREPARE", %{connection: connection} do
    {:ok, %Prepared{id: id}} = Connection.prepare connection, """
      INSERT INTO users (userid, name, age, address, joined_at)
        VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
    """

    assert {:ok, :done} = Connection.execute(connection, id, %{name: "john doe", address: "UK", age: 27})

    assert {:ok, %Prepared{id: id}} = Connection.prepare(connection, "SELECT name, age FROM users WHERE age=? AND address=? ALLOW FILTERING")

    assert {:ok, [%{"name" => "john doe"}]} = Connection.execute(connection, id, [27, "UK"])
  end

  test "DELETE", %{connection: connection} do
    assert {:ok, :done} = Connection.query connection, """
      INSERT INTO users (userid, name, age, address, joined_at)
        VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, data} = Connection.query(connection, "SELECT * FROM users WHERE name='john doe' LIMIT 1 ALLOW FILTERING")
    user_id = data |> hd |> Map.get("userid")

    assert {:ok, %Prepared{id: id}} = Connection.prepare(connection, "DELETE FROM users WHERE userid=?")
    assert {:ok, :done} = Connection.execute(connection, id, [{:uuid, user_id}])
  end

  test "UPDATE", %{connection: connection} do
    assert {:ok, :done} = Connection.query connection, """
      INSERT INTO users (userid, name, age, address, joined_at)
        VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    assert {:ok, data} = Connection.query(connection, "SELECT * FROM users WHERE name='john doe' LIMIT 1 ALLOW FILTERING")
    user_id = data |> hd |> Map.get("userid")

    assert {:ok, %Prepared{id: id}} = Connection.prepare connection, """
      UPDATE users SET age=?, address=? WHERE userid=?
    """

    assert {:ok, :done} = Connection.execute(connection, id, [27, "UK", {:uuid, user_id}])

    assert {:ok, %Prepared{id: id}} = Connection.prepare(connection, "SELECT name,age FROM users WHERE age=? AND address=? ALLOW FILTERING")

    assert {:ok, [%{"name" => "john doe"}]} = Connection.execute(connection, id, [27, "UK"])
  end

  test "ERROR", %{connection: connection} do
    assert {:error, {:invalid, "unconfigured table some_table"}} =
      Connection.query(connection, "SELECT * FROM some_table")
    assert {:error, {:syntax_error, "line 1:0 no viable alternative at input 'SELEC' ([SELEC]...)"}} =
      Connection.query(connection, "SELEC * FROM missing_table;")
  end
end
