defmodule EctoCassandra.AdapterTest do
  use ExUnit.Case, async: true

  import EctoCassandra.Assertions

  use EctoCassandra.Query

  defmodule User do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
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
    User
    |> where([u], u.name == "John")
    |> where([u], u.age >= 27)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users WHERE name = 'John' AND age >= 27")

    User
    |> where([u], u.name == ^"John")
    |> where([u], u.age >= ^27)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users WHERE name = ? AND age >= ?")
  end

  test "and" do
    User
    |> where([u], u.name == "John" and u.age >= 90)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users WHERE name = 'John' AND age >= 90")

    User
    |> where([u], u.name == ^"John" and u.age >= ^90)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users WHERE name = ? AND age >= ?")
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
    User
    |> order_by([u], u.joined_at)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users ORDER BY joined_at")

    User
    |> order_by([u], [u.id, u.joined_at])
    |> select([u], [u.id, u.name])
    |> assert_cql("SELECT id, name FROM users ORDER BY id, joined_at")

    User
    |> order_by([u], [asc: u.id, desc: u.joined_at])
    |> select([u], [u.id, u.name])
    |> assert_cql("SELECT id, name FROM users ORDER BY id, joined_at DESC")

    User
    |> order_by([u], [])
    |> select([u], [u.id, u.name])
    |> assert_cql("SELECT id, name FROM users")
  end

  test "limit and offset" do
    User
    |> limit([u], 3)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users LIMIT 3")

    User
    |> limit([u], ^3)
    |> select([u], u.id)
    |> assert_cql("SELECT id FROM users LIMIT ?")
  end

  test "group by" do
    User
    |> group_by([u], u.cat_id)
    |> select([u], u.name)
    |> assert_cql("SELECT name FROM users GROUP BY cat_id")

    User
    |> group_by([u], [u.cat_id, u.age])
    |> select([u], u.name)
    |> assert_cql("SELECT name FROM users GROUP BY cat_id, age")

    User
    |> group_by([u], [])
    |> select([u], u.name)
    |> assert_cql("SELECT name FROM users")
  end

  test "lock" do
    User
    |> where([u], u.age <= 18)
    |> select([u], u.id)
    |> lock("ALLOW FILTERING")
    |> assert_cql("SELECT id FROM users WHERE age <= 18 ALLOW FILTERING")
  end

  test "string escape" do
    User
    |> where(name: "'\\  ")
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE name = '''\\  '")

    User
    |> where(name: "'")
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE name = ''''")
  end

  test "binary ops" do
    User
    |> where([u], u.age == 20)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age = 20")

    User
    |> where([u], u.age != 21)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age != 21")

    User
    |> where([u], u.age >= 22)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age >= 22")

    User
    |> where([u], u.age <= 23)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age <= 23")

    User
    |> where([u], u.age < 24)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age < 24")

    User
    |> where([u], u.age > 25)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age > 25")
   end

  test "fragments" do
    User
    |> where([u], u.joined_at < fragment("now()"))
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE joined_at < now()")

    query = select(User, [u], fragment(age: 20))
    assert_raise Ecto.QueryError, fn -> cql(query) end
  end

  test "literals" do
    User
    |> where(is_student: true)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE is_student = TRUE")

    User
    |> where(is_student: false)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE is_student = FALSE")

    User
    |> where(name: "John")
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE name = 'John'")

    User
    |> where(age: 20)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age = 20")

    User
    |> where(score: 98.2)
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE score = 98.2")

    User
    |> where(data: as_blob(9_999_999_999_999, :bigint))
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE data = bigintAsBlob(9999999999999)")
  end

  test "nested expressions" do
    from(u in User, [])
    |> where([u], u.age > 0 and (u.age > (-123)) and true)
    |> select([u], u.age)
    |> assert_cql("SELECT age FROM users WHERE age > 0 AND age > -123 AND TRUE")
  end

  test "in expression" do
    User
    |> where([u], u.age in [1, 2, 20])
    |> select([:id])
    |> assert_cql("SELECT id FROM users WHERE age IN (1, 2, 20)")
  end

  test "fragments allow ? to be escaped with backslash" do
    query =
      from(u in User,
        where: fragment("? = \'query\\?\'", u.joined_at),
        select: [:id])

    assert cql(query) == "SELECT id FROM users WHERE joined_at = 'query?'"
  end

  test "update_all operations" do
    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: "Jesse"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = 'Jesse' WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: "John"], inc: [age: -3]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = 'John', age = age + -3 WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: "John"], push: [hobbes: "hiking"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = 'John', hobbes = hobbes + ['hiking'] WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == ^"14c755a0-0bb4-4a26-8724-e7f9d2cd1904" , update: [set: [name: ^"Jack"], pull: [hobbes: ^"hiking"]])
    assert cql(query, :update_all) ==
      "UPDATE users SET name = ?, hobbes = hobbes - [?] WHERE id = ?"
  end

  test "update_all using options" do
    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: "Jesse"]], using: [ttl: 1000])
    assert cql(query, :update_all) ==
      "UPDATE users USING TTL 1000 SET name = 'Jesse' WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: "Jesse"]], using: [timestamp: 100000])
    assert cql(query, :update_all) ==
      "UPDATE users USING TIMESTAMP 100000 SET name = 'Jesse' WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", update: [set: [name: "Jesse"]], using: [ttl: 1000, timestamp: 100000])
    assert cql(query, :update_all) ==
      "UPDATE users USING TTL 1000 AND TIMESTAMP 100000 SET name = 'Jesse' WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"
  end

  test "delete_all" do
    assert cql(from(User), :delete_all) == "TRUNCATE users"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904")
    assert cql(query, :delete_all) == "DELETE FROM users WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904"

    query = from(u in User, where: u.age >= 27)
    assert cql(query, :delete_all) == "DELETE FROM users WHERE age >= 27"

    query = from(u in User, where: u.id == "14c755a0-0bb4-4a26-8724-e7f9d2cd1904", if: :exists)
    assert cql(query, :delete_all) ==
      "DELETE FROM users WHERE id = 14c755a0-0bb4-4a26-8724-e7f9d2cd1904 IF EXISTS"
  end

  test "insert" do
    assert EctoCassandra.insert(nil, "users", [name: "John", age: 27], [:id], [id: :binary_id], []) ==
      "INSERT INTO users (id, name, age) VALUES (now(), ?, ?)"

    assert EctoCassandra.insert("prefix", "users", [name: "Jack", age: 28], [:id], [id: :id], []) ==
      "INSERT INTO prefix.users (id, name, age) VALUES (uuid(), ?, ?)"
  end

  test "update" do
    query = EctoCassandra.update(
      nil,
      "users",
      [name: "John", age: 27],
      [:id],
      [],
      []
    )
    assert query == "UPDATE users SET name = ?, age = ? WHERE id = ?"

    query = EctoCassandra.update(
      "keyspace",
      "users",
      [name: "John", age: 27],
      [:id],
      [],
      []
    )
    assert query == "UPDATE keyspace.users SET name = ?, age = ? WHERE id = ?"
  end

  test "delete" do
    query = EctoCassandra.delete(nil, "users", [:name, :age], [])
    assert query == "DELETE FROM users WHERE name = ? AND age = ?"

    query = EctoCassandra.delete(nil, "users", [:name, :age], [if: :exists])
    assert query == "DELETE FROM users WHERE name = ? AND age = ? IF EXISTS"

    query = EctoCassandra.delete("pre", "users", [:name, :age], [])
    assert query == "DELETE FROM pre.users WHERE name = ? AND age = ?"
  end

  describe "functions" do
    test "token" do
      User
      |> where([u], u.id < token("sometest"))
      |> select([u], as_blob(u.data, :text))
      |> assert_cql("SELECT textAsBlob(data) FROM users WHERE id < token('sometest')")

      User
      |> where([u], u.id < token(^"sometest"))
      |> select([u], as_blob(u.data, :text))
      |> assert_cql("SELECT textAsBlob(data) FROM users WHERE id < token(?)")

      User
      |> where([u], u.id < token(["sometest", ^"other test"]))
      |> select([u], as_blob(u.data, :text))
      |> assert_cql("SELECT textAsBlob(data) FROM users WHERE id < token('sometest', ?)")
    end

    test "cast" do
      User
      |> where([u], u.id < cast("sometest", :timeuuid))
      |> select([u], as_blob(u.data, :text))
      |> assert_cql("SELECT textAsBlob(data) FROM users WHERE id < cast('sometest' as timeuuid)")

      User
      |> where([u], u.id < cast(^"sometest", :timeuuid))
      |> select([u], as_blob(u.data, :text))
      |> assert_cql("SELECT textAsBlob(data) FROM users WHERE id < cast(? as timeuuid)")
    end

    test "uuid" do
      User
      |> where([u], u.cat_id == uuid())
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE cat_id = uuid()")
    end

    test "now" do
      User
      |> where([u], u.joined_at >= now())
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE joined_at >= now()")
    end

    test "timeuuid" do
      User
      |> where([u], u.id >= min_timeuuid("2013-01-01 00:05+0000"))
      |> where([u], u.id <= max_timeuuid("2016-01-01 00:05+0000"))
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE id >= minTimeuuid('2013-01-01 00:05+0000') AND id <= maxTimeuuid('2016-01-01 00:05+0000')")
    end

    test "to date" do
      User
      |> where([u], u.id == to_date("a280d70c-6374-40af-be03-e8e3cd60652e"))
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE id = toDate(a280d70c-6374-40af-be03-e8e3cd60652e)")
    end

    test "to timestamp" do
      User
      |> where([u], u.joined_at >= to_timestamp("2011-02-03T04:05:00.000+0000"))
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE joined_at >= toTimestamp('2011-02-03T04:05:00.000+0000')")
    end

    test "to unix timestamp" do
      User
      |> where([u], u.joined_at >= to_unix_timestamp(^"2011-02-03T04:05:00.000+0000"))
      |> select([u], u.name)
      |> assert_cql("SELECT name FROM users WHERE joined_at >= toUnixTimestamp(?)")
    end
  end

  describe "errors" do
    test "invalid flield name" do
      query = select(User, [:"bad name"])
      assert_raise ArgumentError, ~r/bad identifier/, fn -> cql(query) end
    end

    test "invalid table name" do
      query = select("bad table", [:id])
      assert_raise ArgumentError, ~r/bad table name/, fn -> cql(query) end
    end

    test "not" do
      query =
        User
        |> where([u], not(u.cat_id))
        |> select([u], u.id)

      assert_raise Ecto.QueryError, ~r/Cassandra does not support NOT relation/, fn -> cql(query) end
    end

    test "support locking" do
      query =
        User
        |> lock("FOR UPDATE")
        |> where([u], u.age <= 18)
        |> select([u], u.id)

      assert_raise Ecto.QueryError, ~r/Cassandra does not support locking/, fn -> cql(query) end
    end

    test "is nil" do
      query =
        User
        |> where([u], is_nil(u.age))
        |> select([u], u.id)

      assert_raise Ecto.QueryError, ~r/Cassandra does not support IS NULL relation/, fn -> cql(query) end
    end
  end
end
