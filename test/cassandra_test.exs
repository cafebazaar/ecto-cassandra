defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Client
  alias CQL.Supported
  alias CQL.Result.Void

  setup_all do
    {:ok, client} = Client.start_link([])
    Client.query(client, "drop keyspace elixir_cql_test;")
    Client.query client, """
      create keyspace elixir_cql_test
        with replication = {'class':'SimpleStrategy','replication_factor':1};
    """
    Client.query(client, "USE elixir_cql_test;")
    Client.query client, """
      create table users (
        userid uuid,
        name varchar,
        age int,
        address text,
        joined_at timestamp,
        PRIMARY KEY (userid)
      );
    """
    {:ok, %{client: client}}
  end

  setup %{client: client} do
    Client.query(client, "TRUNCATE users;")
    {:ok, %{client: client}}
  end

  test "OPTIONS", %{client: client} do
    assert %Supported{options: options} = Client.options(client)
    assert ["CQL_VERSION", "COMPRESSION"] = Keyword.keys(options)
  end

  test "REGISTER", %{client: client} do
    assert {:error, %CQL.Error{
      code: :protocol_error,
      message: "Invalid value 'BAD_TYPE' for Type",
    }} = Client.register(client, "BAD_TYPE")

    assert {:ok, stream} = Client.register(client, "SCHEMA_CHANGE")
    task = Task.async(Enum, :take, [stream, 1])
    Client.query client, """
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

  test "INSERT", %{client: client} do
    assert %Void{} = Client.query client, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """
  end

  test "INSERT SELECT", %{client: client} do
    assert %Void{} = Client.query client, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    rows = Client.query(client, "select * from users;")
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end

  test "PREPARE", %{client: client} do
    %{id: id} = Client.prepare client, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), ?, ?, ?, toTimestamp(now()));
    """

    assert %Void{} = Client.execute(client, id, %{name: "john doe", address: "UK", age: 27})

    %{id: id} = Client.prepare(client, "select name,age from users where age=? and address=? ALLOW FILTERING")

    rows = Client.execute(client, id, [27, "UK"])
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end

  test "DELETE", %{client: client} do
    assert %Void{} = Client.query client, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    user= Client.query(client, "select * from users where name='john doe' limit 1 ALLOW FILTERING")
    user_id=
      user
      |> hd
      |> Map.get("userid")

    %{id: id} = Client.prepare(client, "delete from users where userid=?")
    assert %Void{} = Client.execute(client, id, [{:uuid, user_id}])
  end

  test "UPDATE", %{client: client} do
    assert %Void{} = Client.query client, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    user= Client.query(client, "select * from users where name='john doe' limit 1 ALLOW FILTERING")
    user_id=
      user
      |> hd
      |> Map.get("userid")

    %{id: id} = Client.prepare client, """
      update users
        set age=?, address=?
        where userid=?
    """

    assert %Void{} = Client.execute(client, id, [27, "UK", {:uuid, user_id}])

    %{id: id} = Client.prepare(client, "select name,age from users where age=? and address=? ALLOW FILTERING")

    rows = Client.execute(client, id, [27, "UK"])
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end

  test "ERROR", %{client: client} do
    assert %CQL.Error{} = Client.query(client, "select * from some_table")
  end
end
