# Cassandra

[![Build Status](https://travis-ci.org/cafebazaar/elixir-cassandra.svg?branch=master)](https://travis-ci.org/cafebazaar/elixir-cassandra)
[![Hex.pm](https://img.shields.io/hexpm/v/cassandra.svg?maxAge=2592000)](https://hex.pm/packages/cassandra)
[![Hex.pm](https://img.shields.io/hexpm/l/cassandra.svg?maxAge=2592000)](https://github.com/cafebazaar/elixir-cassandra/blob/master/LICENSE.md)

An Elixir driver for Apache Cassandra.

This driver works with Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol.

## Installation

Add `cassandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:cassandra, "~> 0.1.0-beta"}]
end
```

## Quick Start

```elixir
alias Cassandra.Connection

{:ok, conn} = Connection.start_link # Connects to 127.0.0.1:9094 by default

{:ok, _} = Connection.query conn, """
  CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  """

{:ok, _} = Connection.query conn, """
  CREATE TABLE IF NOT EXISTS test.users (
    id uuid,
    name varchar,
    age int,
    PRIMARY KEY (id)
  );
  """

{:ok, insert} = Connection.prepare conn, """
  INSERT INTO test.users (id, name, age) VALUES (uuid(), ?, ?);
  """

users = [
  %{name: "Bilbo", age: 50},
  %{name: "Frodo", age: 33},
  %{name: "Gandolf", age: 2019},
]

users
|> Enum.map(&Task.async(fn -> Connection.execute(conn, insert, &1) end))
|> Enum.map(&Task.await/1)

{:ok, rows} = Connection.query(conn, "SELECT * FROM text.users;")

# {:ok,
# [%{"age" => 2019, "id" => "7ecad341-4b87-466a-a637-5fe7f24ec3a4",
#    "name" => "Gandolf"},
#  %{"age" => 50, "id" => "98788196-b3ee-4174-bfe5-79e04e9c9eaf",
#    "name" => "Bilbo"},
#  %{"age" => 33, "id" => "87af738f-864c-4e18-998f-fdc511263e78",
#    "name" => "Frodo"}]}
```

