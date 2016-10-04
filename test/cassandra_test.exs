defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  import Cassandra.Protocol, only: [run: 2, connect: 1]

  alias CQL.{Options, Supported, Prepare, Execute, QueryParams}
  alias CQL.Result.Void

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
        PRIMARY KEY (userid)
      );
    """
    :ok
  end

  setup do
    {:ok, %{socket: socket}} = connect([])
    run socket, "USE elixir_cql_test;"
    run socket, "TRUNCATE users;"
    {:ok, %{socket: socket}}
  end

  test "OPTIONS", %{socket: socket} do
    assert %Supported{options: options} = run socket, %Options{}
    assert ["CQL_VERSION", "COMPRESSION"] = Keyword.keys(options)
  end

  test "INSERT", %{socket: socket} do
    assert %Void{} = run socket, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
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

  test "PREPARE", %{socket: socket} do
    %{id: id} = run socket, %Prepare{query: """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), ?, ?, ?, toTimestamp(now()));
    """}

    assert %Void{} = run socket, %Execute{
      id: id,
      params: %QueryParams{
        values: ["john doe", 27, "UK"],
        consistency: :ONE,
      }
    }

    %{id: id} = run socket, %Prepare{
      query: "select name,age from users where age=? and address=? ALLOW FILTERING"
    }

   rows = run socket, %Execute{
      id: id,
      params: %QueryParams{
        values: [27, "UK"],
      }
    }
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end

  test "DELETE", %{socket: socket} do
    assert %Void{} = run socket, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    user= run socket, "select * from users where name='john doe' limit 1 ALLOW FILTERING"
    user_id=
      user
      |> hd
      |> Map.get("userid")

    %{id: id} = run socket, %Prepare{query: "delete from users where userid=?"}
    assert %Void{} = run socket, %Execute{
      id: id,
      params: %QueryParams{
        values: [{:uuid, user_id}],
      }
    }
  end

  test "UPDATE", %{socket: socket} do
    assert %Void{} = run socket, """
      insert into users (userid, name, age, address, joined_at)
        values (uuid(), 'john doe', 20, 'US', toTimestamp(now()));
    """

    user= run socket, "select * from users where name='john doe' limit 1 ALLOW FILTERING"
    user_id=
      user
      |> hd
      |> Map.get("userid")

    %{id: id} = run socket, %Prepare{query: """
      update users
        set age=?, address=?
        where userid=?
    """}

    assert %Void{} = run socket, %Execute{
      id: id,
      params: %QueryParams{
        values: [27, "UK", {:uuid, user_id}],
      }
    }

    %{id: id} = run socket, %Prepare{
      query: "select name,age from users where age=? and address=? ALLOW FILTERING"
    }

   rows = run socket, %Execute{
      id: id,
      params: %QueryParams{
        values: [27, "UK"],
      }
    }
    assert Enum.find(rows, fn map -> map["name"] == "john doe" end)
  end
end
