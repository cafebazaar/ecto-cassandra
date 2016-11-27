defmodule EctoCassandra.StorageTest do
  use ExUnit.Case, async: true

  setup do
    options = [
      keyspace: "test",
      contact_points: ["127.0.0.1"],
      replication: [
        class: "SimpleStrategy",
        replication_factor: 1,
      ]
    ]
    {:ok, options: options}
  end

  test "create keyspace", %{options: options} do
    assert EctoCassandra.create_keyspace(options) == join """
        CREATE KEYSPACE test
          WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}
      """
  end

  test "drop keyspace", %{options: options} do
    assert EctoCassandra.drop_keyspace(options) == "DROP KEYSPACE test"
  end

  defp join(str) do
    str
    |> String.replace(~r/\s+/, " ")
    |> String.trim
  end
end
