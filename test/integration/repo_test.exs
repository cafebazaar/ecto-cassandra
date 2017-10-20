defmodule EctoCassandra.Integration.RepoTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias EctoCassandra.Integration.TestRepo
  alias EctoCassandra.Integration.Post

  setup do
    TestRepo.delete_all(Post)
  end

  test "returns already started for started repos" do
    assert {:error, {:already_started, _}} = TestRepo.start_link
  end

  test "fetch empty" do
    assert [] == TestRepo.all(Post)
    assert [] == TestRepo.all(from p in Post)
  end

  test "fetch with in" do
    TestRepo.insert!(%Post{title: "hello"})

    assert []  = TestRepo.all from p in Post, where: p.title in []
    assert []  = TestRepo.all from p in Post, where: p.title in ["1", "2", "3"]
    assert [_] = TestRepo.all from p in Post, where: p.title in ["1", "hello", "3"]
  end

  test "fetch without schema" do
    %Post{} = TestRepo.insert!(%Post{title: "title1"})
    %Post{} = TestRepo.insert!(%Post{title: "title2"})

    titles = TestRepo.all(from(p in "posts", select: p.title))
    assert 2 == Enum.count(titles)
    assert "title1" in titles
    assert "title2" in titles

    assert [_] =
      TestRepo.all(from(p in "posts", where: p.title == "title1", select: p.title))
  end

  @tag :invalid_prefix
  test "fetch with invalid prefix" do
    assert catch_error(TestRepo.all("posts", prefix: "oops"))
  end

  test "insert, update and delete" do
    post = %Post{title: "insert, update, delete"}
    meta = post.__meta__

    assert %Post{} = to_be_updated = TestRepo.insert!(post)
    changeset = Ecto.Changeset.change(to_be_updated, visits: 10)
    assert {:ok, updated} = TestRepo.update(changeset)
    assert updated.updated_at > updated.inserted_at

    deleted_meta = put_in meta.state, :deleted
    assert %Post{} = to_be_deleted = TestRepo.insert!(post)
    assert %Post{__meta__: ^deleted_meta} = TestRepo.delete!(to_be_deleted)

    loaded_meta = put_in meta.state, :loaded
    assert %Post{__meta__: ^loaded_meta} = TestRepo.insert!(post)

    post = TestRepo.one(Post)
    assert post.__meta__.state == :loaded
    assert post.inserted_at
  end

  test "insert, update and delete with invalid prefix" do
    post = TestRepo.insert!(%Post{title: "bar"})
    changeset = Ecto.Changeset.change(post, title: "foo")
    assert catch_error(TestRepo.insert(%Post{}, prefix: "oops"))
    assert catch_error(TestRepo.update(changeset, prefix: "oops"))
    assert catch_error(TestRepo.delete(changeset, prefix: "oops"))
  end

  test "map" do
    meta = %{"example" => "1", "test" => "2"}
    post = %Post{title: "test map", meta: meta}
    assert %Post{} = TestRepo.insert!(post)
    assert [%Post{meta: ^meta}] = TestRepo.all(Post)
  end

  test "inet" do
    ip = {127, 0, 0, 1}
    post = %Post{title: "test inet", ip: ip}
    assert %Post{} = TestRepo.insert!(post)
    assert [%Post{ip: ^ip}] = TestRepo.all(Post)
  end

  test "inet version 6" do
    ip = {0, 0, 0, 0, 0, 0, 0, 1}
    post = %Post{title: "test inet", ip: ip}
    assert %Post{} = TestRepo.insert!(post)
    assert [%Post{ip: ^ip}] = TestRepo.all(Post)
  end
end
