defmodule EctoTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    @foreign_key_type :binary_id

    schema "users" do
      field :name, :string
      field :age,  :integer
    end
  end

  defp cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, Cassandra.Ecto.Adapter, counter)
    query = Ecto.Query.Planner.normalize(query, operation, Cassandra.Ecto.Adapter, counter)
    Cassandra.Ecto.to_cql(query, operation)
  end

  test "from" do
    assert cql(select(User, [u], u.name)) == ~s(SELECT name FROM users)
  end

  test "from without schema" do
    assert cql(select("posts", [p], p.title)) == ~s(SELECT title FROM posts)
    assert cql(select("posts", [:title])) == ~s(SELECT title FROM posts)
  end
end

