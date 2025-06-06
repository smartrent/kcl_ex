defmodule KinesisClient.ShardDetectorTest do
  use ExUnit.Case, async: false
  import Mox

  alias KinesisClient.ShardDetector

  # Define mock for Kinesis module
  Mox.defmock(KinesisClient.KinesisMock, for: KinesisClient.Kinesis.Adapter)

  # Common test variables
  @stream_name "test-stream"
  @stream_arn "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream"

  # Setup test context
  setup :verify_on_exit!

  # Make mocks global to allow recursive calls
  setup :set_mox_global

  # Setup mock for each test
  setup do
    # Store the original module in app env
    original_module = Application.get_env(:kcl_ex, :kinesis_module)

    # Override the kinesis module for tests
    Application.put_env(:kcl_ex, :kinesis_module, KinesisClient.KinesisMock)

    # Restore original module after test
    on_exit(fn ->
      if original_module do
        Application.put_env(:kcl_ex, :kinesis_module, original_module)
      else
        Application.delete_env(:kcl_ex, :kinesis_module)
      end
    end)

    :ok
  end

  describe "list_shards/2" do
    test "returns all shards when there are no more shards" do
      # Mock the describe_stream call to return a response with HasMoreShards = false
      stream_name = @stream_name

      KinesisClient.KinesisMock
      |> expect(:describe_stream, fn ^stream_name, _opts ->
        {:ok,
         %{
           "StreamDescription" => %{
             "HasMoreShards" => false,
             "Shards" => [
               %{"ShardId" => "shard-1"},
               %{"ShardId" => "shard-2"}
             ],
             "StreamName" => @stream_name,
             "StreamARN" => @stream_arn
           }
         }}
      end)

      # Call the function under test
      result = ShardDetector.list_shards(@stream_name)

      # Assert we get the expected result
      assert {:ok,
              %{
                "HasMoreShards" => false,
                "Shards" => [
                  %{"ShardId" => "shard-1"},
                  %{"ShardId" => "shard-2"}
                ],
                "StreamName" => @stream_name,
                "StreamARN" => @stream_arn
              }} = result
    end

    test "makes multiple calls when there are more shards" do
      # Mock the first describe_stream call to return HasMoreShards = true
      stream_name = @stream_name

      KinesisClient.KinesisMock
      |> expect(:describe_stream, fn ^stream_name, _opts ->
        {:ok,
         %{
           "StreamDescription" => %{
             "HasMoreShards" => true,
             "Shards" => [
               %{"ShardId" => "shard-1"},
               %{"ShardId" => "shard-2"}
             ],
             "StreamName" => @stream_name,
             "StreamARN" => @stream_arn
           }
         }}
      end)

      # Mock the second describe_stream call to return HasMoreShards = false
      |> expect(:describe_stream, fn ^stream_name, _opts ->
        {:ok,
         %{
           "StreamDescription" => %{
             "HasMoreShards" => false,
             "Shards" => [
               %{"ShardId" => "shard-3"},
               %{"ShardId" => "shard-4"}
             ],
             "StreamName" => @stream_name,
             "StreamARN" => @stream_arn
           }
         }}
      end)

      # Call the function under test
      result = ShardDetector.list_shards(@stream_name)

      # Assert we get the expected result with all shards combined
      assert {:ok,
              %{
                "HasMoreShards" => false,
                "Shards" => [
                  %{"ShardId" => "shard-3"},
                  %{"ShardId" => "shard-4"},
                  %{"ShardId" => "shard-1"},
                  %{"ShardId" => "shard-2"}
                ],
                "StreamName" => @stream_name,
                "StreamARN" => @stream_arn
              }} = result
    end

    test "accumulates shards from previous calls" do
      # Define some initial shards to pass to the function
      initial_shards = [%{"ShardId" => "shard-0"}]

      # Mock the describe_stream call to return a response
      stream_name = @stream_name

      KinesisClient.KinesisMock
      |> expect(:describe_stream, fn ^stream_name, _opts ->
        {:ok,
         %{
           "StreamDescription" => %{
             "HasMoreShards" => false,
             "Shards" => [
               %{"ShardId" => "shard-1"},
               %{"ShardId" => "shard-2"}
             ],
             "StreamName" => @stream_name,
             "StreamARN" => @stream_arn
           }
         }}
      end)

      # Call the function under test with initial shards
      result = ShardDetector.list_shards(@stream_name, initial_shards)

      # Assert we get the expected result with initial and new shards combined
      assert {:ok,
              %{
                "HasMoreShards" => false,
                "Shards" => [
                  %{"ShardId" => "shard-1"},
                  %{"ShardId" => "shard-2"},
                  %{"ShardId" => "shard-0"}
                ],
                "StreamName" => @stream_name,
                "StreamARN" => @stream_arn
              }} = result
    end

    test "handles error response from Kinesis" do
      # Define a non-existent stream name
      non_existent_stream = "non-existent-stream"

      # Mock the describe_stream call to return an error
      KinesisClient.KinesisMock
      |> expect(:describe_stream, fn ^non_existent_stream, _opts ->
        {:error, %{"__type" => "ResourceNotFoundException", "message" => "Stream not found"}}
      end)

      # Call the function under test and verify it raises a MatchError
      assert_raise MatchError, fn ->
        ShardDetector.list_shards(non_existent_stream)
      end
    end
  end
end
