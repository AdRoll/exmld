defmodule ElixirProcessor.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_processor,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :erlexec],
      included_applications: [:erlmld, :exmld],
      mod: {ElixirProcessor.Application, []}
    ]
  end

  defp deps do
    [
      {:exmld, "~> 1.0.2"}
    ]
  end
end
