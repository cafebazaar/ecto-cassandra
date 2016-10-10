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

{:ok, conn} = Connection.start_link(keyspace: "system_schema")

{:ok, rows} = Connection.query(conn, "SELECT keyspace_name, table_name FROM tables;")

Enum.each rows, fn row ->
  IO.inspect(row)
end
```

