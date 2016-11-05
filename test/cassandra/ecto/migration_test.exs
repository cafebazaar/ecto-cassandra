defmodule EctoMigrationTest do
  use ExUnit.Case, async: true

  import Ecto.Migration

  test "create table" do
    create = {:create, table(:posts), [
      {:add, :id, :id, [partition_key: true]},
      {:add, :name, :string, []},
      {:add, :price, :float, []},
      {:add, :on_hand, :integer, []},
      {:add, :published_at, :timestamp, []},
      {:add, :is_active, :boolean, []},
      {:add, :tags, {:array, :string}, []},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        name text,
        price float,
        on_hand integer,
        published_at timestamp,
        is_active boolean,
        tags LIST<text>,
        PRIMARY KEY (id))
      """
  end

  test "create table with prefix" do
    create = {:create, table(:posts, prefix: :foo), [
      {:add, :id, :id, [partition_key: true]},
      {:add, :category, :string, []}
    ]}

    assert cql(create) == join """
      CREATE TABLE foo.posts (id uuid,
        category text, PRIMARY KEY (id))
      """
  end

  test "create table with comment" do
    create = {:create, table(:posts, comment: "table comment"), [
      {:add, :id, :id, [partition_key: true]},
      {:add, :created_at, :timestamp, []},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        created_at timestamp,
        PRIMARY KEY (id))
      WITH comment='table comment'
      """
  end

  test "create table with composite partition key" do
    create = {:create, table(:posts), [
      {:add, :id, :id, [partition_key: true]},
      {:add, :cat_id, :timeuuid, [partition_key: true]},
      {:add, :name, :string, []},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        cat_id timeuuid,
        name text,
        PRIMARY KEY ((id, cat_id)))
      """
  end

  test "create table with composite primary key" do
    create = {:create, table(:posts), [
      {:add, :id, :id, [partition_key: true]},
      {:add, :cat_id, :timeuuid, [partition_key: true]},
      {:add, :name, :string, [clustering_column: true]},
      {:add, :age, :int, [clustering_column: true]},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        cat_id timeuuid,
        name text,
        age int,
        PRIMARY KEY ((id, cat_id), name, age))
      """
  end

  test "create table with options" do
    create = {:create, table(:posts,
      [options: "WITH CLUSTERING ORDER BY (created_at DESC)"]),[
      {:add, :id, :id, [partition_key: true]},
      {:add, :created_at, :timestamp, []},
      {:add, :name, :string, []},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        created_at timestamp,
        name text,
        PRIMARY KEY (id))
        WITH CLUSTERING ORDER BY (created_at DESC)
      """

    create = {:create, table(:posts,
      [options: "WITH compaction = { 'class' : 'LeveledCompactionStrategy' }"]),[
      {:add, :id, :id, [partition_key: true]},
      {:add, :name, :string, []},
    ]}

    assert cql(create) == join """
      CREATE TABLE posts (id uuid,
        name text,
        PRIMARY KEY (id))
        WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
      """
  end

  test "drop table" do
    drop = {:drop, table(:posts)}
    assert cql(drop) == "DROP TABLE posts"
  end

  test "drop table with prefix" do
    drop = {:drop, table(:posts, prefix: :foo)}
    assert cql(drop) == "DROP TABLE foo.posts"
  end

  test "drop table with if exists" do
    drop = {:drop_if_exists, table(:posts)}
    assert cql(drop) == "DROP TABLE IF EXISTS posts"
  end

  test "alter table add" do
    alter = {:alter, table(:posts),[
                {:add, :name, :ascii, []},
                {:add, :cat_id, :uuid, []},
              ]}

    assert cql(alter) == join """
        ALTER TABLE posts
          ADD name ascii, cat_id uuid
      """
  end

  test "alter table remove" do
    alter = {:alter, table(:posts),[
                {:remove, :name, :ascii, []},
                {:remove, :cat_id, :uuid, []},
              ]}

    assert cql(alter) == join """
        ALTER TABLE posts
          DROP name cat_id
      """
  end

  test "alter table modify" do
    alter = {:alter, table(:posts),
               [{:modify, :name, :ascii, []}]
            }

    assert cql(alter) == join """
        ALTER TABLE posts
          name TYPE ascii
      """
  end

  test "alter table with comments on table" do
    alter = {:alter, table(:posts, comment: "table comment"),
               [{:modify, :name, :ascii, []}]
            }

    assert cql(alter) == join """
        ALTER TABLE posts
          name TYPE ascii
          WITH comment='table comment'
      """
  end

  test "alter table with options" do
    alter = {:alter, table(:posts,
              [options: "WITH compaction = { 'class' : 'LeveledCompactionStrategy' }"]),[
                {:modify, :name, :ascii, []}
            ]}

    assert cql(alter) == join """
        ALTER TABLE posts
          name TYPE ascii
          WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
      """
  end

  test "alter table with prefix" do
    alter = {:alter, table(:posts, prefix: :foo),[
                {:modify, :name, :ascii, []}
            ]}

    assert cql(alter) == join """
        ALTER TABLE foo.posts
          name TYPE ascii
      """
  end

  describe "unsupported errors" do
    test "create table without primary key" do
      create = {:create, table(:posts, comment: "table comment"), [
        {:add, :created_at, :timestamp, []},
      ]}

      assert_raise Ecto.MigrationError, ~r/requires PRIMARY KEY/, fn ->
        cql(create)
      end
    end

    test "create table with references" do
      create = {:create, table(:posts, comment: "table comment"), [
        {:add, :id, :id, [partition_key: true]},
        {:add, :category, references(:category), []},
      ]}

      assert_raise Ecto.MigrationError, ~r/Cassandra does not support references/, fn ->
        cql(create)
      end
    end

    test "create table with comment on columns" do
      create = {:create, table(:posts), [
        {:add, :id, :id, [partition_key: true]},
        {:add, :category, :string, [comment: "colums comment"]},
      ]}

      assert_raise Ecto.MigrationError, ~r/Cassandra does not support columns comment/, fn ->
        cql(create)
      end
    end

    test "alter table with different change types" do
      alter = {:alter, table(:posts),
                 [{:modify, :name, :ascii, []},
                  {:remove, :cat_id, :uuid, []},
                ]}

      assert_raise Ecto.MigrationError, ~r/Cassandra does not support ALTER TABLE with different change types/, fn ->
        cql(alter)
      end
    end

    test "alter table modifi multiple columns" do
      alter = {:alter, table(:posts),[
                 {:modify, :name, :ascii, []},
                 {:modify, :age, :string, []}
              ]}
      assert_raise Ecto.MigrationError, ~r/Cassandra does not support altering multiple columns/, fn ->
        cql(alter)
      end
    end

  end

  defp join(str) do
    str
    |> String.replace(~r/\s+/, " ")
    |> String.trim
  end

  defp cql(definitions) do
    Cassandra.Ecto.ddl(definitions)
  end
end
