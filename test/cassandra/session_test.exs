defmodule Cassandra.SessionTest do
  use ExUnit.Case

  alias Cassandra.{Cluster, Session}

  @moduletag capture_log: true

  @host Cassandra.TestHelper.host
  @keyspace Cassandra.TestHelper.keyspace
  @table_name "people"

  @truncate_table "TRUNCATE #{@table_name};"

  @create_table """
    CREATE TABLE #{@table_name} (
      id uuid,
      name varchar,
      age int,
      PRIMARY KEY (id)
    );
  """

  setup_all do
    {:ok, cluster} = Cluster.start_link([@host])
    {:ok, session} = Session.start_link(cluster, [keyspace: @keyspace])
    {:ok, _} = Session.execute(session, @create_table)

    {:ok, %{session: session}}
  end

  setup %{session: session} do
    {:ok, :done} = Session.execute(session, @truncate_table)
    :ok
  end

  test "execute", %{session: session} do
    {:ok, %CQL.Result.Rows{}} = Session.execute(session, "SELECT * FROM system_schema.tables")
  end

  test "prepare", %{session: session} do
    insert = "INSERT INTO people (id, name, age) VALUES (now(), ?, ?);"
    assert {:ok, ^insert} = Session.prepare(session, insert)

    characters = [
      %{name: "Bilbo", age: 50},
      %{name: "Frodo", age: 33},
      %{name: "Gandolf", age: 2019},
    ]

    assert characters
      |> Enum.map(&Session.execute(session, insert, values: &1))
      |> Enum.map(&match?({:ok, _}, &1))
      |> Enum.all?

    assert {:ok, rows} = Session.execute(session, "SELECT name, age FROM people;")
    assert %CQL.Result.Rows{rows_count: 3, columns: ["name", "age"]} = rows

    for char <- characters do
      assert !is_nil(Enum.find(rows.rows, fn [name, age] -> name == char[:name] and age == char[:age] end))
    end
  end

  test "send", %{session: session} do
    assert {:ok, %CQL.Supported{}} = Session.send(session, %CQL.Options{})
  end
end
