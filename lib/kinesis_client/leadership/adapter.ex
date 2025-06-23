defmodule KinesisClient.Leadership.Adapter do
  @moduledoc """
  Behavior for DynamoDB operations needed for leader election.
  """

  @callback put_leader_item(String.t(), String.t(), integer(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}

  @callback update_leader_heartbeat(String.t(), String.t(), integer(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}

  @callback get_leader_info(String.t(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}

  @callback describe_table(String.t(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}

  @callback create_table(String.t(), String.t(), map(), integer(), integer(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}

  @callback delete_leader_item(String.t(), String.t(), Keyword.t()) ::
              {:ok, map()} | {:error, any()}
end
