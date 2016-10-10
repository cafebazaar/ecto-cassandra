defmodule Cassandra.Mixfile do
  use Mix.Project

  def project, do: [
    app: :cassandra,
    version: "0.1.1-beta",
    elixir: "~> 1.3",
    build_embedded: Mix.env == :prod,
    start_permanent: Mix.env == :prod,
    description: "A pure Elixir driver for Apache Cassandra",
    package: package,
    deps: deps,
  ]

  def application, do: [
    applications: [:logger],
  ]

  defp deps, do: [
    {:connection, "~> 1.0"},
    {:uuid, "~> 1.1"},
    {:excheck, "~> 0.5", only: :test},
    {:triq, github: "triqng/triq", only: :test},
  ]

  defp package, do: [
    licenses: ["Apache 2.0"],
    maintainers: ["Ali Rajabi", "Hassan Zamani"],
    links: %{
      "Github" => "https://github.com/cafebazaar/elixir-cassandra",
    },
    files: ~w(mix.exs lib README.md LICENSE.md),
  ]
end
