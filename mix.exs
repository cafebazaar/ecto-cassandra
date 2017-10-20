defmodule EctoCassandra.Mixfile do
  use Mix.Project

  def project, do: [
    app: :ecto_cassandra,
    version: "1.0.0-rc.3",
    elixir: "~> 1.4",
    build_embedded: Mix.env == :prod,
    start_permanent: Mix.env == :prod,
    test_coverage: [tool: ExCoveralls],
    preferred_cli_env: [
      "coveralls": :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.html": :test,
      "coveralls.travis": :test,
    ],
    source_url: "https://github.com/cafebazaar/ecto-cassandra",
    description: "Cassandra Adapter for Ecto",
    package: package(),
    deps: deps(),
  ]

  def application, do: [
    applications: [:logger, :cassandra],
  ]

  defp deps, do: [
    {:ecto, "~> 2.1.0"},
    {:cassandra, "~> 1.0.0-rc.1"},
    {:excoveralls, "~> 0.6", only: :test},
    {:ex_doc, ">= 0.0.0", only: :dev},
    {:lz4, github: "szktty/erlang-lz4", override: true}, # TODO check if fixed remove
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
