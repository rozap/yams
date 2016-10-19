defmodule Yams.Mixfile do
  use Mix.Project

  def project do
    [app: :yams,
     version: "0.2.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     description: description,
     package: [
        licenses: ["MIT"],
        links: %{
          "GitHub" => "https://github.com/rozap/yams"
        },
        maintainers: ["rozap"]
     ],
     deps: deps()]
  end

  defp description do
    """
      A tiny wrapper around leveldb for timeseries data
    """
  end

  def application do
    [applications: [:logger, :eleveldb]]
  end

  defp deps do
    [
      {:eleveldb, github: "basho/eleveldb", tag: "2.2.19"},
      {:statistics, "~> 0.4.1"},
      {:uuid, "~> 1.1.4"},
    ]
  end
end
