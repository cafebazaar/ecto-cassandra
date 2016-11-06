use Mix.Config

config :cassandra,
  ecto_repos: [Repo]

config :cassandra, Repo,
  adapter: Cassandra.Ecto.Adapter,
  keyspace: "test",
  contact_points: ["127.0.0.1"],
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1,
  ]
