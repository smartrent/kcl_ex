defmodule KinesisClient.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kinesis_client,
      version: "1.0.0-rc.0",
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      description: description(),
      compilers: [:unused] ++ Mix.compilers(),
      test_coverage: [tool: ExCoveralls],
      deps: deps(),
      source_url: "https://github.com/uberbrodt/kcl_ex",
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],
      unused: [
        ignore: [
          {KinesisClient.HierarchicalShardSyncer.Supervisor, :child_spec, 1},
          {KinesisClient.HierarchicalShardSyncer.Supervisor, :start_link, 1},
          {KinesisClient.HierarchicalShardSyncer.Task, :child_spec, 1},
          {KinesisClient.HierarchicalShardSyncer.Task, :start_link, 1},
          {KinesisClient.LeaderElection, :child_spec, 1},
          {KinesisClient.LeaderElection, :start_link, 1},
          {KinesisClient.LeaderElection.Supervisor, :child_spec, 1},
          {KinesisClient.LeaderElection.Supervisor, :start_link, 1},
          {KinesisClient.LeaseCoordinator, :child_spec, 1},
          {KinesisClient.LeaseCoordinator, :start_link, 1},
          {KinesisClient.LeaseCoordinatorSupervisor, :child_spec, 1},
          {KinesisClient.LeaseCoordinatorSupervisor, :start_link, 1},
          {KinesisClient.Stream.ShardManager, :child_spec, 1},
          {KinesisClient.Stream.ShardManager, :start_link, 1},
          {KinesisClient.Stream, :child_spec, 1},
          {KinesisClient.Stream, :start_link, 1},
          {KinesisClient.Stream.LeaseRefresher, :child_spec, 1},
          {KinesisClient.Stream.LeaseRefresher, :start_link, 1},
          {KinesisClient.Stream.LeaseRefresherSupervisor, :child_spec, 1},
          {KinesisClient.Stream.LeaseRefresherSupervisor, :start_link, 1},
          {KinesisClient.Stream.Shard, :child_spec, 1},
          {KinesisClient.Stream.Shard, :start_link, 1},
          {KinesisClient.Stream.Shard.Lease, :child_spec, 1},
          {KinesisClient.Stream.Shard.Lease, :start_link, 1},
          {KinesisClient.Stream.Shard.Pipeline, :child_spec, 1},
          {KinesisClient.Stream.Shard.Pipeline, :start_link, 1},
          {KinesisClient.Stream.Shard.Producer, :child_spec, 1},
          {KinesisClient.Stream.Shard.Producer, :start_link, 1},
          {KinesisClient.Telemetry, :child_spec, 1},
          {KinesisClient.Telemetry, :start_link, 1},
          {KinesisClient.WorkerRegistry, :child_spec, 1},
          {KinesisClient.WorkerRegistry, :start_link, 1},
          {KinesisClient.WorkerRegistrySupervisor, :child_spec, 1},
          {KinesisClient.WorkerRegistrySupervisor, :start_link, 1}
        ]
      ]
    ]
  end

  def description do
    """
    A pure Elixir implementation of the AWS Java Kinesis Client Library (KCL)
    """
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :wx, :observer, :runtime_tools]
    ]
  end

  defp package do
    [
      licenses: ["Apache 2.0"],
      maintainers: ["Chris Brodt"],
      links: %{Github: "https://github.com/uberbrodt/kcl_ex"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.2"},
      {:configparser_ex, "~> 4.0"},
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ex_aws, "~> 2.0"},
      {:ex_aws_dynamo, "~> 4.2"},
      {:ex_aws_kinesis, "~> 2.0"},
      {:excoveralls, "~> 0.18.5", only: :test},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:hackney, "~> 1.9"},
      {:jason, "~> 1.1"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:mox, "~> 0.5", only: :test},
      {:retry, "~> 0.18"},
      # Telemetry packages for metrics
      {:telemetry, "~> 1.2", override: true},
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.0"},
      {:telemetry_metrics_prometheus, "~> 1.1"},
      {:ex_hash_ring, "~> 6.0"},
      {:mix_unused, "~> 0.4.1"},
      {:mimic, "~> 1.12", only: :test},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false}
    ]
  end
end
