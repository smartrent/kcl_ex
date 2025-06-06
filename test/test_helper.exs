Application.put_env(:ex_aws, :dynamodb,
  scheme: "http://",
  host: "localhost",
  port: "4566",
  region: "us-east-1"
)

Application.put_env(:ex_aws, :kinesis,
  scheme: "http://",
  host: "localhost",
  port: "4568",
  region: "us-east-1"
)

Logger.configure(level: :warn)

# Define mocks for tests
Mox.defmock(KinesisClient.Leadership.AdapterMock, for: KinesisClient.Leadership.Adapter)
Mox.defmock(KinesisClient.Stream.AppStateMock, for: KinesisClient.Stream.AppState.Adapter)
Mox.defmock(KinesisClient.KinesisMock, for: KinesisClient.Kinesis.Adapter)
Mox.defmock(KinesisClient.WorkerRegistryMock, for: KinesisClient.Worker.Adapter)

# Make sure these modules can be mocked with Mimic
Mimic.copy(KinesisClient.ShardDetector)
Mimic.copy(KinesisClient.Stream.AppState)
Mimic.copy(KinesisClient.LeaderElection)
Mimic.copy(KinesisClient.WorkerRegistry)

ExUnit.start()
