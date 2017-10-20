defmodule Exmld.Mixfile do
  use Mix.Project

  def project do
    [
      app: :exmld,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps()
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
      {:flow, "~> 0.12.0"},
      {:erlmld, git: "https://github.com/AdRoll/erlmld.git", tag: "v0.1.1"},
      {:ex_doc, "~> 0.16", only: :dev, runtime: false}
    ]
  end
end
