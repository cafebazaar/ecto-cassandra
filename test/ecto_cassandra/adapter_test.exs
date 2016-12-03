defmodule EctoCassandra.AdapterTest do
  use ExUnit.Case, async: true

  use EctoCassandra.Query

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    @foreign_key_type :binary_id

    schema "users" do
      field :cat_id, Ecto.UUID
      field :name, :string
      field :age,  :integer
      field :is_student, :boolean
      field :score, :float
      field :data, :binary
      field :hobbes, {:array, :string}
      field :joined_at, Ecto.DateTime
    end
  end

  test "from" do
    assert cql(select(User, [u], u.name)) == "SELECT name FROM users"
  end

  test "from without schema" do
    assert cql(select("some_table", [s], s.x)) == "SELECT x FROM some_table"
    assert cql(select("some_table", [:y])) == "SELECT y FROM some_table"
  end

  test "select" do
    query = select(User, [u], {u.name, u.age})
    assert cql(query) == "SELECT name, age FROM users"

    query = select(User, [u], struct(u, [:name, :age]))
    assert cql(query) == "SELECT name, age FROM users"
  end

  test "aggregates" do
    query = select(User, [u], count(u.name))
    assert cql(query) == "SELECT count(name) FROM users"
  end

  test "where" do
    query =
      User
      |> where([u], u.name == "John")
      |> where([u], u.age >= 27)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users WHERE name = 'John' AND age >= 27"

    query =
      User
      |> where([u], u.name == ^"John")
      |> where([u], u.age <= ^27)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users WHERE name = ? AND age <= ?"
  end

  test "and" do
    query =
      User
      |> where([u], u.name == "John" and u.age >= 90)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users WHERE name = 'John' AND age >= 90"
  end

  test "or" do
    assert_raise Ecto.QueryError, ~r/Cassandra does not support OR operator/, fn ->
      cql(from u in User, where: u.name == "Jack", or_where: u.age > 10, select: u.name)
    end

    assert_raise Ecto.QueryError, ~r/Cassandra does not support OR operator/, fn ->
      cql(from u in User, where: u.name == "Jack" or u.age > 10, select: u.name)
    end
  end

  test "order by" do
    query =
      User
      |> order_by([u], u.joined_at)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users ORDER BY joined_at"

    query =
      User
      |> order_by([u], [u.id, u.joined_at])
      |> select([u], [u.id, u.name])
    assert cql(query) == "SELECT id, name FROM users ORDER BY id, joined_at"

    query =
      User
      |> order_by([u], [asc: u.id, desc: u.joined_at])
      |> select([u], [u.id, u.name])
    assert cql(query) == "SELECT id, name FROM users ORDER BY id, joined_at DESC"

    query =
      User
      |> order_by([u], [])
      |> select([u], [u.id, u.name])
    assert cql(query) == "SELECT id, name FROM users"
  end

  test "limit and offset" do
    query =
      User
      |> limit([u], 3)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users LIMIT 3"

    query =
      User
      |> limit([u], ^3)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users LIMIT ?"
  end

  test "group by" do
    query =
      User
      |> group_by([u], u.cat_id)
      |> select([u], u.name)
    assert cql(query) == "SELECT name FROM users GROUP BY cat_id"

    query =
      User
      |> group_by([u], [u.cat_id, u.age])
      |> select([u], u.name)
    assert cql(query) == "SELECT name FROM users GROUP BY cat_id, age"

    query =
      User
      |> group_by([u], [])
      |> select([u], u.name)
    assert cql(query) == "SELECT name FROM users"
  end

  test "lock" do
    query =
      User
      |> lock("ALLOW FILTERING")
      |> where([u], u.age <= 18)
      |> select([u], u.id)
    assert cql(query) == "SELECT id FROM users WHERE age <= 18 ALLOW FILTERING"
  end

  test "string escape" do
    query =
      User
      |> where(name: "'\\  ")
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE name = '''\\  '"

    query =
      User
      |> where(name: "'")
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE name = ''''"
  end

  test "binary ops" do
    query =
      User
      |> where([u], u.age == 20)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age = 20"

    query =
      User
      |> where([u], u.age != 21)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age != 21"

    query =
      User
      |> where([u], u.age >= 22)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age >= 22"

    query =
      User
      |> where([u], u.age <= 23)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age <= 23"

    query =
      User
      |> where([u], u.age < 24)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age < 24"

    query =
      User
      |> where([u], u.age > 25)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age > 25"
   end

  test "fragments" do
    query =
      User
      |> where([u], u.joined_at < fragment("now()"))
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE joined_at < now()"

    query = select(User, [u], fragment(age: 20))
    assert_raise Ecto.QueryError, fn ->
      cql(query)
    end
  end

  test "literals" do
    query =
      User
      |> where(is_student: true)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE is_student = TRUE"

    query =
      User
      |> where(is_student: false)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE is_student = FALSE"

    query =
      User
      |> where(name: "John")
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE name = 'John'"

    query =
      User
      |> where(age: 20)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age = 20"

    query =
      User
      |> where(score: 98.2)
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE score = 98.2"

    query =
      User
      |> where(data: as_blob(9_999_999_999_999, :bigint))
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE data = bigintAsBlob(9999999999999)"
  end

  test "nested expressions" do
    query =
      from(u in User, [])
      |> where([u], u.age > 0 and (u.age > ^(-123)) and true)
      |> select([u], u.age)
    assert cql(query) == "SELECT age FROM users WHERE age > 0 AND age > ? AND TRUE"
  end

  test "in expression" do
    query =
      User
      |> where([u], u.age in [1, 2, 20])
      |> select([:id])
    assert cql(query) == "SELECT id FROM users WHERE age IN (1, 2, 20)"
  end

  test "fragments allow ? to be escaped with backslash" do
    query =
      from(u in User,
        where: fragment("? = \"query\\?\"", u.joined_at),
        select: [:id])

    result =
      "SELECT id FROM users" <>
      " WHERE joined_at = \"query?\""

    assert cql(query) == String.rstrip(result)
  end

  test "update_all" do
    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: "Jesse"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = 'Jesse' WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: ^"Fredric"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = ? WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: ^"John"], inc: [age: -3]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = ?, age = age + -3 WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: ^"John"], push: [hobbes: "hiking"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = ?, hobbes = hobbes + [ 'hiking' ] WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: "Jack"], pull: [hobbes: "hiking"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = 'Jack', hobbes = hobbes - [ 'hiking' ] WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"
  end

  test "delete_all" do
    assert cql(from(User), :delete_all) == "TRUNCATE users"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904")
    assert cql(query, :delete_all) ==
      "DELETE FROM users WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.age >= 27)
    assert cql(query, :delete_all) ==
      "DELETE FROM users WHERE age >= 27"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904")
    assert cql(query, :delete_all, if: :exists) ==
      "DELETE FROM users WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904 IF EXISTS"
  end

  test "insert" do
    assert EctoCassandra.insert(nil, "users", [name: "John", age: 27], [:id], [id: :binary_id], []) ==
      {"INSERT INTO users (id, name, age) VALUES (now(), 'John', 27)", []}

    assert EctoCassandra.insert("prefix", "users", [name: "Jack", age: 28], [:id], [id: :id], []) ==
      {"INSERT INTO prefix.users (id, name, age) VALUES (uuid(), 'Jack', 28)", []}

    assert EctoCassandra.insert("prefix", "users", [id: :now, name: "Jack", age: 29, inserted_at: :now], [], [], []) ==
      {"INSERT INTO prefix.users (id, name, age, inserted_at) VALUES (now(), 'Jack', 29, now())", []}
  end

  test "update" do
    query = EctoCassandra.update(
      nil,
      "users",
      [name: "John", age: 27],
      [id: "4a00d739-63ce-42ad-a200-b214429f7559"],
      [],
      []
    )
    assert query == {"UPDATE users SET name = 'John', age = 27 WHERE id = 4a00d739-63ce-42ad-a200-b214429f7559", []}

    query = EctoCassandra.update(
      "u",
      "users",
      [name: "John", age: 27],
      [id: "4a00d739-63ce-42ad-a200-b214429f7559"],
      [],
      []
    )
    assert query == {"UPDATE u.users SET name = 'John', age = 27 WHERE id = 4a00d739-63ce-42ad-a200-b214429f7559", []}
  end

  test "delete" do
    query = EctoCassandra.delete(nil, "users", [name: "John", age: 27], [])
    assert query == {"DELETE FROM users WHERE name = 'John' AND age = 27", []}

    query = EctoCassandra.delete("pre", "users", [name: "John", age: 28], [])
    assert query == {"DELETE FROM pre.users WHERE name = 'John' AND age = 28", []}
  end

  describe "functions" do
    test "token" do
      query =
        User
        |> where([u], u.id < token("sometest"))
        |> select([u], as_blob(u.data, :text))
      assert cql(query) == "SELECT textAsBlob(data) FROM users WHERE id < token('sometest')"

      query =
        User
        |> where([u], u.id < token(^"sometest"))
        |> select([u], as_blob(u.data, :text))
      assert cql(query) == "SELECT textAsBlob(data) FROM users WHERE id < token(?)"

      query =
        User
        |> where([u], u.id < token([^"sometest", "other test"]))
        |> select([u], as_blob(u.data, :text))
      assert cql(query) == "SELECT textAsBlob(data) FROM users WHERE id < token(?, 'other test')"

      query =
        User
        |> where([u], u.id < token([^"sometest", ^"other test"]))
        |> select([u], as_blob(u.data, :text))
      assert cql(query) == "SELECT textAsBlob(data) FROM users WHERE id < token(?, ?)"
    end

    test "cast" do
      query =
        User
        |> where([u], u.id < cast("sometest", :timeuuid))
        |> select([u], as_blob(u.data, :text))
      assert cql(query) == "SELECT textAsBlob(data) FROM users WHERE id < cast('sometest' as timeuuid)"
    end

    test "uuid" do
      query =
        User
        |> where([u], u.cat_id == uuid())
        |> select([u], u.name)
      assert cql(query) == "SELECT name FROM users WHERE cat_id = uuid()"
    end

    test "now" do
      query =
        User
        |> where([u], u.joined_at >= now())
        |> select([u], u.name)
      assert cql(query) == "SELECT name FROM users WHERE joined_at >= now()"
    end

    test "timeuuid" do
      query =
        User
        |> where([u], u.id >= min_timeuuid("2013-01-01 00:05+0000"))
        |> where([u], u.id <= max_timeuuid("2016-01-01 00:05+0000"))
        |> select([u], u.name)
      assert cql(query) ==
        "SELECT name FROM users WHERE id >= minTimeuuid('2013-01-01 00:05+0000') AND id <= maxTimeuuid('2016-01-01 00:05+0000')"
    end

    test "to date" do
      query =
        User
        |> where([u], u.id == to_date("a280d70c-6374-40af-be03-e8e3cd60652e"))
        |> select([u], u.name)
      assert cql(query) == "SELECT name FROM users WHERE id = toDate(a280d70c-6374-40af-be03-e8e3cd60652e)"
    end

    test "to timestamp" do
      query =
        User
        |> where([u], u.joined_at >= to_timestamp("2011-02-03T04:05:00.000+0000"))
        |> select([u], u.name)
      assert cql(query) == "SELECT name FROM users WHERE joined_at >= toTimestamp('2011-02-03T04:05:00.000+0000')"
    end

    test "to unix timestamp" do
      query =
        User
        |> where([u], u.joined_at >= to_unix_timestamp("2011-02-03T04:05:00.000+0000"))
        |> select([u], u.name)
      assert cql(query) == "SELECT name FROM users WHERE joined_at >= toUnixTimestamp('2011-02-03T04:05:00.000+0000')"
    end
  end

  describe "errors" do
    test "invalid flield name" do
      query = select(User, [:"bad name"])
      assert_raise ArgumentError, ~r/bad identifier/, fn ->
        cql(query)
      end
    end

    test "invalid table name" do
      query = select("bad table", [:id])
      assert_raise ArgumentError, ~r/bad table name/, fn ->
        cql(query)
      end
    end

    test "not" do
      query =
        User
        |> where([u], not(u.cat_id))
        |> select([u], u.id)
      assert_raise Ecto.QueryError, ~r/Cassandra does not support NOT relation/, fn ->
        cql(query)
      end
    end

    test "support locking" do
      query =
        User
        |> lock("FOR UPDATE")
        |> where([u], u.age <= 18)
        |> select([u], u.id)
      assert_raise Ecto.QueryError, ~r/Cassandra does not support locking/, fn ->
        cql(query)
      end
    end

    test "is nil" do
      query =
        User
        |> where([u], is_nil(u.age))
        |> select([u], u.id)
      assert_raise Ecto.QueryError, ~r/Cassandra does not support IS NULL relation/, fn ->
        cql(query)
      end
    end
  end

  defp cql(query, operation \\ :all, options \\ [], counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, EctoCassandra.Adapter, counter)
    query = Ecto.Query.Planner.normalize(query, operation, EctoCassandra.Adapter, counter)
    EctoCassandra.to_cql(query, operation, options)
  end
end
