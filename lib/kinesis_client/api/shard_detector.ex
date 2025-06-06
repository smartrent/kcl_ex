defmodule KinesisClient.ShardDetector do
  alias KinesisClient.Kinesis

  def list_shards(stream_name, shard_list \\ []) do
    kinesis_module = Application.get_env(:kcl_ex, :kinesis_module, Kinesis)
    {:ok, result} = kinesis_module.describe_stream(stream_name, [])

    case result do
      %{"StreamDescription" => %{"HasMoreShards" => true, "Shards" => shards}} ->
        list_shards(
          stream_name,
          shards ++ shard_list
        )

      %{"StreamDescription" => %{"HasMoreShards" => false, "Shards" => shards} = stream} ->
        {:ok, stream |> Map.put("Shards", shards ++ shard_list)}
    end
  end
end
