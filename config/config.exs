use Mix.Config

config :cassandra, Repo,
  adapter: Cassandra.Ecto.Adapter,
  keyspace: "test",
  contact_points: ["172.17.0.2"]

