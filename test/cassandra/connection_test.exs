defmodule Cassandra.ConnectionTest do
  use ExUnit.Case

  alias Cassandra.Connection

  defp query(str, options \\ []) do
    CQL.encode(%CQL.Query{query: str, params: struct(CQL.QueryParams, options)})
  end

  @moduletag capture_log: true

  @host     Cassandra.TestHelper.host
  @keyspace Cassandra.TestHelper.keyspace
  @table_name "users"

  @truncate_table %CQL.Query{
    query: "TRUNCATE #{@table_name};"
  }

  @create_table %CQL.Query{
    query: """
      CREATE TABLE #{@table_name} (
        id uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (id)
      );
    """
  }

  setup_all do
    {:ok, conn} = Connection.start_link(host: @host, async_init: false, keyspace: @keyspace)
    {:ok, _} = Connection.send(conn, @create_table)
    {:ok, %{conn: conn}}
  end

  setup %{conn: conn} do
    {:ok, :done} = Connection.send(conn, @truncate_table)
    :ok
  end

  describe "sync init" do
    test "connection_failed" do
      assert {:error, :connection_failed} = Connection.start(port: 9111, async_init: false)
    end

    test "keyspace_error" do
      assert {:error, :keyspace_error} = Connection.start(host: @host, async_init: false, keyspace: "not_existing_keyspace")
    end
  end

  describe "async init" do
    test "max_attempts" do
      assert {:ok, pid} = Connection.start(port: 9111, connect_timeout: 50, reconnection_args: [max_attempts: 2])
      ref = Process.monitor(pid)
      assert {:error, :not_connected} = Connection.send(pid, "")
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :max_attempts}}, 1000
    end

    test "keyspace_error" do
      assert {:ok, pid} = Connection.start(host: @host, keyspace: "not_existing_keyspace")
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :keyspace_error}}, 1000
    end
  end

  describe "send" do
    test ":done", %{conn: conn} do
      assert {:ok, :done} = Connection.send(conn, @truncate_table)
    end

    test ":ready", %{conn: conn} do
      request = CQL.encode(%CQL.Register{})
      assert {:ok, :ready} = Connection.send(conn, request)
    end

    test "SetKeyspace", %{conn: conn} do
      assert {:ok, %CQL.Result.SetKeyspace{name: @keyspace}} =
        Connection.send(conn, query("USE #{@keyspace};"))
    end

    test "SchemaChange", %{conn: conn} do
      request = query("DROP TABLE IF EXISTS names;")
      assert {:ok, _} = Connection.send(conn, request)
      request = query """
        CREATE TABLE names (
          id uuid,
          name varchar,
          PRIMARY KEY (id)
        );
      """
      assert {:ok, %CQL.Result.SchemaChange{}} = Connection.send(conn, request)
    end

    test "Supported", %{conn: conn} do
      request = CQL.encode(%CQL.Options{})
      assert {:ok, %CQL.Supported{}} = Connection.send(conn, request)
    end

    test "Prepared", %{conn: conn} do
      prepare = %CQL.Prepare{query: "SELECT * FROM #{@table_name};"}
      assert {:ok, %CQL.Result.Prepared{}} = Connection.send(conn, prepare)

      request = CQL.encode(prepare)
      assert {:ok, %CQL.Result.Prepared{}} = Connection.send(conn, request)
    end

    test "Rows", %{conn: conn} do
      request = %CQL.Prepare{query: "INSERT INTO #{@table_name} (id, name, age) VALUES (now(), ?, ?);"}
      assert {:ok, insert} = Connection.send(conn, request)

      request = %CQL.Execute{prepared: insert, params: %CQL.QueryParams{values: ["John", 32]}}
      assert {:ok, :done} = Connection.send(conn, request)

      request = %CQL.Query{query: "SELECT name, age FROM #{@table_name};"}
      assert {:ok, %CQL.Result.Rows{rows_count: 1, columns: ["name", "age"], rows: [["John", 32]]}} =
        Connection.send(conn, request)
    end

    test "stream", %{conn: conn} do
      request = %CQL.Query{
        query: "SELECT table_name FROM system_schema.tables;",
        params: %CQL.QueryParams{page_size: 2},
      }
      assert {:ok, %CQL.Result.Rows{rows: rows, rows_count: nil}} = Connection.send(conn, request)
      assert %GenEvent.Stream{} = rows
      assert Enum.count(rows) > 1
    end

    test "invalid", %{conn: conn} do
      assert {:error, :invalid} = Connection.send(conn, <<1, 1, 1>>)
    end

    test "CQL Error", %{conn: conn} do
      assert {:error, {:syntax_error, message}} =
        Connection.send(conn, query("SELEC * FROM tests;"))
      assert message =~ "'SELEC'"

      assert {:error, {:invalid, "unconfigured table not_existing_table"}} =
        Connection.send(conn, query("SELECT * FROM not_existing_table;"))
    end

    test "CQL Events" do
      assert {:ok, conn} = Connection.start_link(host: @host, async_init: false, keyspace: @keyspace, event_manager: self)
      assert {:ok, :ready} = Connection.send(conn, %CQL.Register{})

      create = query """
        CREATE TABLE event_test (
          id uuid,
          PRIMARY KEY (id)
        );
      """
      assert {:ok, _} = Connection.send(conn, create)
      assert_receive {:"$gen_cast", {:notify, %CQL.Event{}}}
    end
  end

  describe "send_async" do
    test "invalid", %{conn: conn} do
      ref = Connection.send_async(conn, <<1, 1, 1>>)
      assert is_reference(ref)
      assert_receive {^ref, {:error, :invalid}}
    end
  end
end
