defmodule Exmld.Mixfile do
  use Mix.Project

  @version "0.1.10"
  @name "exmld"
  @repo "https://github.com/AdRoll/#{@name}"

  def project do
    [
      app: :exmld,
      version: @version,
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps(),
      package: package(),
      docs: [source_ref: "v#{@version}",
             source_url: @repo],
      description: "An Elixir library for processing multiple Kinesis and " <>
      "DynamoDB streams and shards in a single node using the Kinesis " <>
      "Client Library and MultiLangDaemon."
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      applications: [:flow]
    ]
  end

  defp deps do
    [
      {:flow, "~> 0.14.2"},
      {:erlmld, "~> 0.1.12"},
      {:ex_doc, "~> 0.16", only: :dev, runtime: false}
    ]
  end

  defp package do
    %{
      name: @name,
      licenses: ["BSD 3-Clause License"],
      maintainers: ["AdRoll RTB team <rtb-team+#{@name}@adroll.com>"],
      links: %{"GitHub" => @repo}
    }
  end
end
