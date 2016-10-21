defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Connection
  alias Cassandra.ConnectionTest, as: Conn
  alias CQL.Supported

  setup_all do
    {:ok, _} = Conn.query("USE elixir_cassandra_test;")
    {:ok, _} = Conn.query("""
      CREATE TABLE users (
        id uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (id)
      );
    """)

    :ok
  end

  setup do
    {:ok, :done} = Conn.query("TRUNCATE users;")
    :ok
  end

  describe "#start" do
    # @tag capture_log: true
    # test ":max_attempts option" do
    #   {:ok, conn} = Connection.start(port: 9111, max_attempts: 1)
    #   Process.monitor(conn)
    #   assert_receive {:DOWN, _, :process, ^conn, :max_attempts}
    # end

    @tag capture_log: true
    test ":async_init option" do
      assert {:error, :connection_failed} = Connection.start(port: 9111, async_init: false)
    end
  end

  test "#options" do
    assert {:ok, %Supported{options: options}} = Conn.send(%CQL.Options{})
    assert ["COMPRESSION", "CQL_VERSION"] = Keyword.keys(options)
  end

  describe "#query" do
    test "returns :done when there result do not contain any rows" do
      assert {:ok, :done} =
        Conn.query """
          INSERT INTO users (id, name, age, address, joined_at)
            VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
        """
    end

    test "returns result rows as a list" do
      assert {:ok, :done} =
        Conn.query """
          INSERT INTO users (id, name, age, address, joined_at)
            VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
        """

      assert {:ok, [%{"name" => "john doe"}]} = Conn.query("SELECT name FROM users;")
    end

    # test "returns result as a stream when there are more pages" do
    #   assert {:stream, stream} =
    #     Conn.send(%CQL.Query{
    #       query: "SELECT keyspace_name FROM system_schema.tables;",
    #       params: %CQL.QueryParams{page_size: 2},
    #     })

    #   assert true =
    #     stream
    #     |> Stream.map(&Map.keys/1)
    #     |> Enum.any?(&(&1 == ["keyspace_name"]))
    # end

    test "returns {:error, {code, reason}} on error" do
      assert {:error, {:invalid, "unconfigured table some_table"}} =
        Conn.query("SELECT * FROM some_table")

      assert {:error, {:syntax_error, "line 1:0 no viable alternative at input 'SELEC' ([SELEC]...)"}} =
        Conn.query("SELEC * FROM missing_table;")
    end

    # test "forbids using bind marker" do
    #   assert {:error, {:invalid, "Query string can not contain bind marker `?`, use parepare instead"}} =
    #     Conn.query "INSERT INTO users (id, name) VALUES (?, ?);"
    # end

    test "handles large results repeatedly" do
      assert {:ok, _} =
        Conn.query "SELECT * FROM system.local;"
      assert {:ok, _} =
        Conn.query "SELECT * FROM system.local;"
      assert {:ok, _} =
        Conn.query "SELECT * FROM system.local;"
    end

    test "handles nil values" do
      assert {:ok, _} =
        Conn.query "INSERT INTO users (id, name, age) VALUES (uuid(), 'Jack', 121);"
      assert {:ok, [%{"address" => nil}]} =
        Conn.query "SELECT * FROM users;"
    end
  end

  # describe "#register" do
  #   test "accepts invalid types" do
  #     assert {:error, {:protocol_error, "Invalid value 'BAD_TYPE' for Type"}} =
  #       Conn.send(%CQL.Register{types: ["BAD_TYPE"]})
  #   end

  #   test "returns a stream of events" do
  #     assert {:stream, stream} = Conn.send(%CQL.Register{types: ["SCHEMA_CHANGE"]})
  #     task = Task.async(Enum, :take, [stream, 1])
  #     Conn.query """
  #       CREATE TABLE event_test (
  #         id uuid,
  #         PRIMARY KEY (id)
  #       );
  #     """

  #     assert [%CQL.Event{
  #       type: "SCHEMA_CHANGE",
  #       info: %{
  #         change: "CREATED",
  #         target: "TABLE",
  #         options: %{keyspace: "elixir_cassandra_test", table: "event_test"},
  #       },
  #     }] = Task.await(task)
  #   end
  # end

  describe "#prepare" do
    test "returns {:ok, prepared} with valid query" do
      assert {:ok, _} = Conn.prepare """
        INSERT INTO users (id, name, age, address, joined_at)
          VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
      """
    end

    test "returns {:error, {code, reason}} on error" do
      assert {:error, {:invalid, "unconfigured table some_table"}} =
        Conn.prepare """
          INSERT INTO some_table (id, name, age, address, joined_at)
            VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
        """
    end
  end

  describe "#execute" do
    test "returns :done when there result do not contain any rows" do
      {:ok, prepared} = Conn.prepare """
        INSERT INTO users (id, name, age, address, joined_at)
          VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
      """

      assert {:ok, :done} = Conn.execute(prepared, %{name: "john doe", address: "UK", age: 27})
    end

    test "returns result rows as a list" do
      {:ok, prepared} = Conn.prepare """
        INSERT INTO users (id, name, age, address, joined_at)
          VALUES (uuid(), ?, ?, ?, toTimestamp(now()));
      """
      {:ok, :done} = Conn.execute(prepared, %{name: "john doe", address: "UK", age: 27})
      {:ok, prepared} = Conn.prepare("SELECT name, age FROM users WHERE age=? AND address=? ALLOW FILTERING")

      assert {:ok, [%{"name" => "john doe"}]} = Conn.execute(prepared, [27, "UK"])
    end
  end

  describe "Data Manipulation" do
    test "DELETE" do
      assert {:ok, :done} = Conn.query """
        INSERT INTO users (id, name, age, address, joined_at)
          VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
      """

      assert {:ok, data} = Conn.query("SELECT * FROM users WHERE name='john doe' LIMIT 1 ALLOW FILTERING")

      user_id = data |> hd |> Map.get("id")
      assert {:ok, prepared} = Conn.prepare("DELETE FROM users WHERE id=?")

      assert {:ok, :done} = Conn.execute(prepared, [user_id])
    end

    test "UPDATE" do
      assert {:ok, :done} = Conn.query """
        INSERT INTO users (id, name, age, address, joined_at)
          VALUES (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
      """

      assert {:ok, data} = Conn.query("SELECT * FROM users WHERE name='john doe' LIMIT 1 ALLOW FILTERING")
      user_id = data |> hd |> Map.get("id")

      assert {:ok, prepared} = Conn.prepare """
        UPDATE users SET age=?, address=? WHERE id=?
      """

      assert {:ok, :done} = Conn.execute(prepared, [27, "UK", user_id])

      assert {:ok, prepared} = Conn.prepare("SELECT name, age FROM users WHERE age=? AND address=? ALLOW FILTERING")

      assert {:ok, [%{"name" => "john doe"}]} = Conn.execute(prepared, [27, "UK"])
    end

    test "concurrent actions" do
      {:ok, insert} = Conn.prepare """
        INSERT INTO elixir_cassandra_test.users (id, name, age) VALUES (uuid(), ?, ?);
        """

      assert true =
        1..10
        |> Enum.map(&Task.async(fn -> Conn.execute(insert, ["user-#{&1}", &1 + 40]) end))
        |> Enum.map(&Task.await(&1, :infinity))
        |> Enum.all?(&(&1 == {:ok, :done}))
    end
  end
end
