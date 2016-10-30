defmodule EctoTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    @foreign_key_type :binary_id

    schema "users" do
      field :cat_id, Ecto.UUID
      field :name, :string
      field :age,  :integer
      field :joined_at, Ecto.DateTime
    end
  end

  test "from" do
    assert cql(select(User, [u], u.name)) == ~s(SELECT name FROM users)
  end

  test "from without schema" do
    assert cql(select("some_table", [s], s.x)) == ~s(SELECT x FROM some_table)
    assert cql(select("some_table", [:y])) == ~s(SELECT y FROM some_table)
  end

  test "select" do
    query = select(User, [u], {u.name, u.age})
    assert cql(query) == ~s{SELECT name, age FROM users}

    query = select(User, [u], struct(u, [:name, :age]))
    assert cql(query) == ~s{SELECT name, age FROM users}
  end
  defp cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, Cassandra.Ecto.Adapter, counter)
    query = Ecto.Query.Planner.normalize(query, operation, Cassandra.Ecto.Adapter, counter)
    Cassandra.Ecto.to_cql(query, operation)
  end
