defmodule EctoCassandra.Mixfile do
  use Mix.Project

  def project, do: [
    app: :ecto_cassandra,
    version: "1.0.0-beta",
    elixir: "~> 1.3",
    build_embedded: Mix.env == :prod,
    start_permanent: Mix.env == :prod,
    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      "coveralls": :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.html": :test,
    ],
    source_url: "https://github.com/cafebazaar/ecto-cassandra",
    description: "Cassandra Adapter for Ecto",
    package: package,
    deps: deps,
  ]

  def application, do: [
    applications: [:logger],
  ]

  defp deps, do: [
    {:ecto, "~> 2.1.0-rc.3"},
    {:cassandra, github: "cafebazaar/elixir-cassandra"},
    {:excoveralls, "~> 0.5", only: :test},
  ]

  defp package, do: [
    licenses: ["Apache 2.0"],
    maintainers: ["Ali Rajabi", "Hassan Zamani"],
    links: %{
      "Github" => "https://github.com/cafebazaar/ecto-cassandra",
    },
    files: ~w(mix.exs lib README.md LICENSE.md),
  ]
end
