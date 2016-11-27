defmodule EctoCassandra.Integration.TypesTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias EctoCassandra.Integration.{TestRepo, Post, User}

  setup do
    TestRepo.delete_all(User)
    TestRepo.delete_all(Post)
  end

  test "primitive types" do
    integer  = 1
    float    = 0.1
    title    = "types test"
    uuid     = "00010203-0405-0607-0809-0a0b0c0d0e0f"
    boolean  = true
    datetime = ~N[2014-01-16 20:26:51.000]

    TestRepo.insert!(%Post{title: title, public: boolean, visits: integer, uuid: uuid,
                           counter: integer, inserted_at: datetime, intensity: float})

    # nil
    assert [nil] = TestRepo.all(from p in Post, select: p.ip)

    # ID
    assert [1] = TestRepo.all(from p in Post, where: p.counter == ^integer, select: p.counter, lock: "ALLOW FILTERING")

    # Integers
    assert [1] = TestRepo.all(from p in Post, where: p.visits == ^integer, select: p.visits, lock: "ALLOW FILTERING")
    assert [1] = TestRepo.all(from p in Post, where: p.visits == 1, select: p.visits, lock: "ALLOW FILTERING")

    # Floats
    assert [0.1] = TestRepo.all(from p in Post, where: p.intensity == ^float, select: p.intensity, lock: "ALLOW FILTERING")
    assert [0.1] = TestRepo.all(from p in Post, where: p.intensity == 0.1, select: p.intensity, lock: "ALLOW FILTERING")

    # Booleans
    assert [true] = TestRepo.all(from p in Post, where: p.public == ^boolean, select: p.public, lock: "ALLOW FILTERING")
    assert [true] = TestRepo.all(from p in Post, where: p.public == true, select: p.public, lock: "ALLOW FILTERING")

    # UUID
    assert [^uuid] = TestRepo.all(from p in Post, where: p.uuid == ^uuid, select: p.uuid, lock: "ALLOW FILTERING")

    # NaiveDatetime
    assert [^datetime] = TestRepo.all(from p in Post, where: p.inserted_at == ^datetime, select: p.inserted_at, lock: "ALLOW FILTERING")

    # Datetime
    datetime = DateTime.utc_now |> Map.update(:microsecond, {0, 0}, fn {x, _} -> {div(x, 1000) * 1000, 3} end)
    TestRepo.insert!(%User{inserted_at: datetime})
    assert [^datetime] = TestRepo.all(from u in User, where: u.inserted_at == ^datetime, select: u.inserted_at, lock: "ALLOW FILTERING")
  end

  @tag :capture_log
  test "aggregated types" do
    datetime = ~N[2014-01-16 20:26:51.000]
    TestRepo.insert!(%Post{title: "aggregated type test", inserted_at: datetime})
    query = from p in Post, select: max(p.inserted_at)
    assert [^datetime] = TestRepo.all(query)
  end

  test "uuid types" do
    assert %Post{} = post = TestRepo.insert!(%Post{title: "bid test", uuid: UUID.uuid4, timeuuid: UUID.uuid1})
    uuid = post.uuid
    timeuuid = post.timeuuid
    assert [[^uuid, ^timeuuid]] = TestRepo.all(from p in Post, select: [p.uuid, p.timeuuid])
  end
end
