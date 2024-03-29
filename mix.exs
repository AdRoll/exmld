defmodule Exmld.Mixfile do
  use Mix.Project

  @version "1.0.4"
  @name "exmld"
  @repo "https://github.com/AdRoll/#{@name}"

  def project do
    [
      app: :exmld,
      version: @version,
      elixir: "~> 1.12",
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
    ]
  end

  defp deps do
    [
      {:flow, "~> 1.2"},
      {:erlmld, "~> 1.0.2"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false}
    ]
  end

  defp package do
    %{
      name: @name,
      licenses: ["BSD-3-Clause"],
      maintainers: ["AdRoll RTB team <rtb-team+#{@name}@adroll.com>"],
      links: %{"GitHub" => @repo}
    }
  end
end
