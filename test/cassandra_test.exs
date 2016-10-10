defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Connection
  alias CQL.Supported

  setup_all do
    {:ok, conn} = Connection.start_link(keyspace: "elixir_cassandra_test")
    {:ok, _} = Connection.query conn, """
      CREATE TABLE users (
        userid uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (userid)
      );
    """

    {:ok, %{conn: conn}}
  end

  setup %{conn: conn} do
    {:ok, :done} = Connection.query(conn, "TRUNCATE users;")
    {:ok, %{conn: conn}}
  end

  test "#options", %{conn: conn} do
    assert {:ok, %Supported{options: options}} = Connection.options(conn)
    assert ["COMPRESSION", "CQL_VERSION"] = Keyword.keys(options)
  end

  describe "#query" do

    test "returns :done when there result do not contain any rows", %{conn: conn} do
      assert {:ok, :done} =
        Connection.query conn, """
          INSERT INTO users (userid, name, age, address, joined_at)
            VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
        """
    end

    test "returns result rows as a list", %{conn: conn} do
      assert {:ok, :done} =
        Connection.query conn, """
          INSERT INTO users (userid, name, age, address, joined_at)
            VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
        """

      assert {:ok, [%{"name" => "john doe"}]} = Connection.query(conn, "SELECT name FROM users;")
    end

    test "returns result as a stream when there are more pages", %{conn: conn} do
      assert {:stream, stream} =
        Connection.query conn, "SELECT keyspace_name FROM system_schema.tables;", page_size: 2

      assert true =
        stream
        |> Stream.map(&Map.keys/1)
        |> Enum.any?(&(&1 == ["keyspace_name"]))
    end

    test "returns {:error, {code, reason}} on error", %{conn: conn} do
      assert {:error, {:invalid, "unconfigured table some_table"}} =
        Connection.query(conn, "SELECT * FROM some_table")

      assert {:error, {:syntax_error, "line 1:0 no viable alternative at input 'SELEC' ([SELEC]...)"}} =
        Connection.query(conn, "SELEC * FROM missing_table;")
    end

    test "forbids using bind marker", %{conn: conn} do
      assert {:error, {:invalid, "Query string can not contain bind marker `?`, use parepare instead"}} =
        Connection.query conn, "INSERT INTO users (id, name) VALUES (?, ?);"
    end
  end

  describe "#register" do

    test "accepts invalid types", %{conn: conn} do
      assert {:error, {:protocol_error, "Invalid value 'BAD_TYPE' for Type"}} =
        Connection.register(conn, "BAD_TYPE")
    end

    test "returns a stream of events", %{conn: conn} do
      assert {:stream, stream} = Connection.register(conn, "SCHEMA_CHANGE")
      task = Task.async(Enum, :take, [stream, 1])
      Connection.query conn, """
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
          options: %{keyspace: "elixir_cassandra_test", table: "event_test"},
        },
      }] = Task.await(task)
    end
  end


    describe "#prepare" do

      test "returns {:ok, prepared} with valid query", %{conn: conn} do
        assert {:ok, prepared} = Connection.prepare conn, """
          INSERT INTO users (userid, name, age, address, joined_at)
            VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
        """
      end

      test "returns {:error, {code, reason}} on error", %{conn: conn} do
        assert {:error, {:invalid, "unconfigured table some_table"}} =
          Connection.prepare conn, """
            INSERT INTO some_table (userid, name, age, address, joined_at)
              VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
          """
      end
    end

    describe "#execute" do

      test "returns :done when there result do not contain any rows", %{conn: conn} do
        {:ok, prepared} = Connection.prepare conn, """
          INSERT INTO users (userid, name, age, address, joined_at)
            VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
        """

        assert {:ok, :done} = Connection.execute(conn, prepared, %{name: "john doe", address: "UK", age: 27})
      end

      test "returns result rows as a list", %{conn: conn} do
        {:ok, prepared} = Connection.prepare conn, """
          INSERT INTO users (userid, name, age, address, joined_at)
            VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
        """
        {:ok, :done} = Connection.execute(conn, prepared, %{name: "john doe", address: "UK", age: 27})
        {:ok, prepared} = Connection.prepare(conn, "SELECT name, age FROM users WHERE age=? AND address=? ALLOW FILTERING")

        assert {:ok, [%{"name" => "john doe"}]} = Connection.execute(conn, prepared, [27, "UK"])
      end
    end
end
