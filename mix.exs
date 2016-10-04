defmodule Cassandra.Mixfile do
  use Mix.Project

  def project, do: [
    app: :cassandra,
    version: "0.1.0",
    elixir: "~> 1.3",
    build_embedded: Mix.env == :prod,
    start_permanent: Mix.env == :prod,
    deps: deps,
  ]

  def application, do: [
    applications: [:logger],
  ]

  defp deps, do: [
    {:connection, "~> 1.0"},
    {:uuid, "~> 1.1"},
  ]
end
