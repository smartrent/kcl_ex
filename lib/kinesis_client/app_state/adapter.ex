defmodule KinesisClient.Stream.AppState.Adapter do
  @moduledoc """
  This interface specifies what the AppState store needs to support.
  """

  alias KinesisClient.Stream.AppState.ShardLease

  @doc """
  Implement to setup any backend storage. Should not clear data as this will be called everytime a
  `KinesisClient.Stream.Coordinator` process is started.
  """
  @callback initialize(app_name :: String.t(), opts :: Keyword.t()) :: :ok | {:error, any}

  @callback get_lease(app_name :: String.t(), shard_id :: String.t(), opts :: Keyword.t()) ::
              ShardLease.t() | :not_found | {:error, any}

  @callback create_lease(
              app_name :: String.t(),
              shard_id :: String.t()
            ) ::
              :ok | :already_exists | {:error, any}

  @callback update_checkpoint(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              checkpoint :: String.t(),
              opts :: Keyword.t()
            ) ::
              :ok | {:error, :lease_owner_match} | {:error, any}

  @callback renew_lease(
              app_name :: String.t(),
              shard_lease :: ShardLease.t()
            ) :: {:ok, integer} | {:error, :lease_renew_failed} | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer | nil,
              opts :: Keyword.t(),
              lease_status :: String.t()
            ) :: {:ok, integer} | {:error, atom} | {:error, atom, String.t()}

  @callback close_shard(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: Keyword.t()
            ) :: :ok | {:error, :lease_owner_match} | {:error, any}

  @callback list_all_leases(app_name :: String.t()) ::
              [ShardLease.t()]

  @callback list_worker_leases(
              app_name :: String.t(),
              lease_owner :: String.t()
            ) :: [ShardLease.t()]

  @callback list_available_leases(app_name :: String.t()) ::
              [ShardLease.t()]

  @callback list_completed_leases(app_name :: String.t()) ::
              [ShardLease.t()]

  @callback list_active_leases(app_name :: String.t()) ::
              [ShardLease.t()]
end
