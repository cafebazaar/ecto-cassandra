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

  test "aggregates" do
    query = select(User, [u], count(u.name))
    assert cql(query) == ~s{SELECT count(name) FROM users}
  end

  test "where" do
    query =
      User
      |> where([u], u.name == "John")
      |> where([u], u.age >= 27)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE name = 'John' AND age >= 27}

    name = "John"
    age = 27
    query =
      User
      |> where([u], u.name == ^name)
      |> where([u], u.age <= ^age)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE name = ? AND age <= ?}
  end

  test "and" do
    query =
      User
      |> where([u], u.name == "John" and u.age >= 90)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE name = 'John' AND age >= 90}
  end

  test "or" do
    assert_raise Ecto.QueryError, ~r/Cassandra do not support OR operator/, fn ->
      IO.inspect cql(from u in User, where: u.name == "Jack", or_where: u.age > 10, select: u.name)
    end
  end

  test "order by" do
    query =
      User
      |> order_by([u], u.joined_at)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users ORDER BY joined_at}

    query =
      User
      |> order_by([u], [u.id, u.joined_at])
      |> select([u], [u.id, u.name])
    assert cql(query) == ~s{SELECT id, name FROM users ORDER BY id, joined_at}

    query =
      User
      |> order_by([u], [asc: u.id, desc: u.joined_at])
      |> select([u], [u.id, u.name])
    assert cql(query) == ~s{SELECT id, name FROM users ORDER BY id, joined_at DESC}

    query =
      User
      |> order_by([u], [])
      |> select([u], [u.id, u.name])
    assert cql(query) == ~s{SELECT id, name FROM users}
  end

  test "limit and offset" do
    query =
      User
      |> limit([u], 3)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users LIMIT 3}
  end

  test "group by" do
    query =
      User
      |> group_by([u], u.cat_id)
      |> select([u], u.name)
    assert cql(query) == ~s{SELECT name FROM users GROUP BY cat_id}

    query =
      User
      |> group_by([u], 2)
      |> select([u], u.name)
    assert cql(query) == ~s{SELECT name FROM users GROUP BY 2}

    query =
      User
      |> group_by([u], [u.cat_id, u.age])
      |> select([u], u.name)
    assert cql(query) == ~s{SELECT name FROM users GROUP BY cat_id, age}

    query =
      User
      |> group_by([u], [])
      |> select([u], u.name)
    assert cql(query) == ~s{SELECT name FROM users}
  end

  test "lock" do
    query =
      User
      |> lock("ALLOW FILTERING")
      |> where([u], u.age <= 18)
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE age <= 18 ALLOW FILTERING}
  end

  test "string escape" do
    query =
      User
      |> where(name: "'\\  ")
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE name = '''\\  '}

    query =
      User
      |> where(name: "'")
      |> select([u], u.id)
    assert cql(query) == ~s{SELECT id FROM users WHERE name = ''''}
  end

  defp cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, Cassandra.Ecto.Adapter, counter)
    query = Ecto.Query.Planner.normalize(query, operation, Cassandra.Ecto.Adapter, counter)
    Cassandra.Ecto.to_cql(query, operation)
  end
end
