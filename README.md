# EctoCassandra

[![Build Status](https://travis-ci.org/cafebazaar/ecto-cassandra.svg?branch=master)](https://travis-ci.org/cafebazaar/ecto-cassandra)
[![Hex.pm](https://img.shields.io/hexpm/v/ecto_cassandra.svg?maxAge=2592000)](https://hex.pm/packages/ecto_cassandra)
[![Hex.pm](https://img.shields.io/hexpm/l/ecto_cassandra.svg?maxAge=2592000)](https://github.com/cafebazaar/ecto-cassandra/blob/master/LICENSE.md)
[![Coverage Status](https://coveralls.io/repos/github/cafebazaar/ecto-cassandra/badge.svg?branch=master)](https://coveralls.io/github/cafebazaar/ecto-cassandra?branch=master)

Cassandra Adapter for [Ecto](https://github.com/elixir-ecto/ecto) (the language integrated query for Elixir)

## Example

```elixir
# In your config/config.exs file
config :my_app, ecto_repos: [Sample.Repo]

config :my_app, Sample.Repo,
  adapter: EctoCassandra.Adapter,
  keyspace: "ecto_simple",
  contact_points: ["localhost"],
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1,
  ]

# In your application code
defmodule Sample.Repo.Migrations.CreateUser do
  use Ecto.Migration

  def change do
    create table(:users, primary_key: false) do
      add :id, :id, partition_key: true
      add :age, :integer, clustring_column: true
      add :name, :string
      add :email, :string
      add :password_hash, :string

      timestamps()
    end
  end
end

defmodule Sample.Repo do
  use Ecto.Repo, otp_app: :my_app
end

defmodule Sample.User do
  use Ecto.Schema

  @primary_key false
  schema "users" do
    field :username, primary_key: true
    field :age, :integer
    field :name # Defaults to type :string
    field :email
    field :password_hash
    field :password, :string, virtual: true
  end
end

defmodule Sample.App do
  import Ecto.Query
  alias Sample.{Repo, User}

  def keyword_query do
    Repo.all from u in User,
      where: u.username == "john",
      select: u.email
  end

  def pipe_query do
    User
    |> where([u], u.age > 10)
    |> order_by(:age)
    |> limit(10)
    |> Repo.all
  end

  def get_by_name do
    Repo.get_by(username: "john")
  end
end
```

