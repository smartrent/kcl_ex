defmodule KinesisClient.Stream.AppState.Dynamo do
  @moduledoc false
  alias KinesisClient.Stream.AppState.Adapter, as: AppStateAdapter
  alias ExAws.Dynamo
  require Logger

  @behaviour AppStateAdapter

  @impl AppStateAdapter
  def initialize(app_name, _opts) do
    case confirm_table_created(app_name) do
      :ok ->
        ensure_gsi_exists(app_name)
        migrate_lease_status_field(app_name)

      {:error, {"ResourceNotFoundException", _}} ->
        create_table(app_name)
        ensure_gsi_exists(app_name)
    end
  end

  defp create_table(app_name) do
    case Dynamo.create_table(app_name, "shard_id", %{shard_id: :string}, 10, 10)
         |> ExAws.request() do
      {:ok, %{}} ->
        confirm_table_created(app_name)

      {:error, {"ResourceInUseException", "Cannot create preexisting table"}} ->
        confirm_table_created(app_name)
    end
  end

  defp confirm_table_created(app_name, attempts \\ 1) do
    case Dynamo.describe_table(app_name) |> ExAws.request() do
      {:ok, %{"Table" => %{"TableStatus" => "CREATING"}}} ->
        case attempts do
          x when x <= 5 -> confirm_table_created(app_name, attempts + 1)
          _ -> raise "could not create dynamodb table!"
        end

      {:ok, _x} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  @impl AppStateAdapter
  def create_lease(app_name, shard_lease) do
    update_opt = [condition_expression: "attribute_not_exists(shard_id)"]

    case Dynamo.put_item(app_name, shard_lease, update_opt)
         |> ExAws.request() do
      {:ok, _} ->
        :ok

      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        :already_exists

      output ->
        output
    end
  end

  @impl AppStateAdapter
  def get_lease(app_name, shard_id, _opts) do
    case Dynamo.get_item(app_name, %{"shard_id" => shard_id}) |> ExAws.request() do
      {:ok, %{"Item" => _} = item} -> item |> decode_item()
      {:ok, _} -> :not_found
      other -> other
    end
  end

  @impl AppStateAdapter
  def renew_lease(app_name, %{shard_id: shard_id, lease_count: lease_count} = shard_lease) do
    updated_count = lease_count + 1
    current_time = System.system_time(:millisecond)

    update_opt = [
      condition_expression: "lease_count = :lc AND lease_owner = :lo",
      expression_attribute_values: %{
        lc: lease_count,
        lo: shard_lease.lease_owner,
        new_lease_count: updated_count,
        last_renewal_time: current_time
      },
      update_expression:
        "SET lease_count = :new_lease_count, last_renewal_time = :last_renewal_time",
      return_values: "UPDATED_NEW"
    ]

    case Dynamo.update_item(app_name, %{"shard_id" => shard_id}, update_opt) |> ExAws.request() do
      {:ok, %{"Attributes" => %{"lease_count" => _}}} -> {:ok, updated_count}
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_renew_failed}
      reply -> reply
    end
  end

  @impl AppStateAdapter
  def take_lease(app_name, shard_id, new_lease_owner, _opts, lease_status \\ "LEASED") do
    current_lease = get_lease(app_name, shard_id, [])
    current_time = System.system_time(:millisecond)

    actual_lease_count = current_lease.lease_count
    updated_count = actual_lease_count + 1

    lease_status =
      cond do
        new_lease_owner == "CHILD_WAITING" -> "CHILD_WAITING"
        lease_status == nil -> if new_lease_owner == "NO_OWNER", do: "AVAILABLE", else: "LEASED"
        true -> lease_status
      end

    if current_lease.lease_owner == "CHILD_WAITING" && new_lease_owner == "CHILD_WAITING" &&
         current_lease.lease_status == "CHILD_WAITING" do
      {:ok, actual_lease_count}
    else
      is_lease_expired = is_lease_expired?(current_lease, current_time, 30000)
      is_lease_balanced = current_lease.lease_owner != new_lease_owner

      if is_lease_expired || is_lease_balanced do
        update_opt = [
          condition_expression: "lease_count = :lc",
          expression_attribute_values: %{
            lc: actual_lease_count,
            lo: new_lease_owner,
            new_lease_count: updated_count,
            current_time: current_time,
            lease_status: lease_status
          },
          update_expression:
            "SET lease_count = :new_lease_count, lease_owner = :lo, last_renewal_time = :current_time, lease_status = :lease_status",
          return_values: "UPDATED_NEW"
        ]

        case Dynamo.update_item(app_name, %{"shard_id" => shard_id}, update_opt)
             |> ExAws.request() do
          {:ok, %{"Attributes" => %{"lease_count" => _}}} ->
            Logger.info(
              "Took lease from #{current_lease.lease_owner}, new_lease_owner: #{new_lease_owner}",
              ansi_color: :green
            )

            {:ok, updated_count}

          {:error, {"ConditionalCheckFailedException", _}} ->
            {:error, :lease_take_failed, current_lease.lease_owner}

          reply ->
            reply
        end
      else
        {:error, :shard_already_claimed}
      end
    end
  end

  defp is_lease_expired?(lease, current_time, expiry_ms) do
    case lease do
      %{last_renewal_time: nil} ->
        true

      %{last_renewal_time: last_time} ->
        current_time - last_time > expiry_ms

      _ ->
        false
    end
  end

  @impl AppStateAdapter
  def update_checkpoint(app_name, shard_id, lease_owner, checkpoint, _opts) do
    update_opt = [
      condition_expression: "lease_owner = :lo",
      expression_attribute_values: %{
        lo: lease_owner,
        checkpoint_num: checkpoint
      },
      update_expression: "SET checkpoint = :checkpoint_num",
      return_values: "UPDATED_NEW"
    ]

    case Dynamo.update_item(app_name, %{"shard_id" => shard_id}, update_opt) |> ExAws.request() do
      {:ok, %{"Attributes" => %{"checkpoint" => %{"S" => ^checkpoint}}}} -> :ok
      {:error, {"ConditionalCheckFailedException", _}} -> {:error, :lease_owner_match}
      reply -> reply
    end
  end

  @impl AppStateAdapter
  def close_shard(app_name, shard_id, lease_owner) do
    update_opt = [
      condition_expression: "lease_owner = :lo",
      expression_attribute_values: %{
        lo: lease_owner,
        completed_v: true,
        status: "COMPLETED"
      },
      update_expression: "SET completed = :completed_v, lease_status = :status",
      return_values: "UPDATED_NEW"
    ]

    case Dynamo.update_item(app_name, %{"shard_id" => shard_id}, update_opt) |> ExAws.request() do
      {:ok, %{"Attributes" => %{"completed" => %{"BOOL" => true}}}} ->
        Logger.info("Successfully marked shard #{shard_id} as COMPLETED")
        :ok

      {:error, {"ConditionalCheckFailedException", _}} ->
        Logger.warning("Failed to mark shard #{shard_id} as completed: lease owner mismatch")
        {:error, :lease_owner_match}

      reply ->
        Logger.error("Error marking shard #{shard_id} as completed: #{inspect(reply)}")
        reply
    end
  end

  @impl AppStateAdapter
  def list_all_leases(app_name) do
    request = ExAws.Dynamo.scan(app_name)

    case ExAws.request(request) do
      {:ok, %{"Items" => items}} ->
        items |> Enum.map(&decode_item/1)

      error ->
        Logger.error("Failed to list leases: #{inspect(error)}")
        []
    end
  end

  @impl AppStateAdapter
  def list_worker_leases(app_name, lease_owner) do
    # Try using GSI first
    request =
      ExAws.Dynamo.query(
        app_name,
        expression_attribute_values: %{:lo => lease_owner, :ls => "LEASED"},
        key_condition_expression: "lease_status = :ls AND lease_owner = :lo",
        index_name: "LeaseStatusIndex"
      )

    case ExAws.request(request) do
      {:ok, %{"Items" => items}} ->
        items |> Enum.map(&decode_item/1)

      {:error, {"ResourceNotFoundException", "Index not found"}} ->
        Logger.error(
          "LeaseStatusIndex GSI not created. How?  I thought we were waiting on this to be created before supervision tree continues"
        )

        []

      error ->
        Logger.error("Failed to list worker leases: #{inspect(error)}")
        []
    end
  end

  defp list_leases_by_status(app_name, status) do
    request =
      ExAws.Dynamo.query(
        app_name,
        expression_attribute_values: %{:ls => status},
        key_condition_expression: "lease_status = :ls",
        index_name: "LeaseStatusIndex"
      )

    case ExAws.request(request) do
      {:ok, %{"Items" => items}} ->
        items |> Enum.map(&decode_item/1)

      error ->
        Logger.error("Failed to list leases by status: #{inspect(error)}")
        []
    end
  end

  @impl AppStateAdapter
  def list_available_leases(app_name) do
    list_leases_by_status(app_name, "AVAILABLE")
  end

  @impl AppStateAdapter
  def list_completed_leases(app_name) do
    list_leases_by_status(app_name, "COMPLETED")
  end

  @impl AppStateAdapter
  def list_active_leases(app_name) do
    list_leases_by_status(app_name, "LEASED")
  end

  defp decode_item(item) do
    item
    |> Dynamo.decode_item(as: KinesisClient.Stream.AppState.ShardLease)
  end

  # Migrate existing records to include lease_status field
  defp migrate_lease_status_field(app_name) do
    Logger.info("Checking for records that need lease_status field...")

    # Get all records using scan (since we may not have GSI yet)
    request = ExAws.Dynamo.scan(app_name)

    case ExAws.request(request) do
      {:ok, %{"Items" => items}} ->
        # Filter for items without lease_status or with null lease_status
        items_to_update =
          Enum.filter(items, fn item ->
            not Map.has_key?(item, "lease_status")
          end)

        total_items = length(items)
        items_to_update_count = length(items_to_update)

        Logger.info(
          "Found #{total_items} total records, #{items_to_update_count} need lease_status field"
        )

        # Update each item to add lease_status field based on completed status
        Enum.each(items_to_update, fn item ->
          # Handle potential different formats for shard_id
          shard_id =
            case item["shard_id"] do
              %{"S" => id} ->
                id

              id when is_binary(id) ->
                id

              _ ->
                Logger.error("Invalid shard_id format: #{inspect(item["shard_id"])}")
                nil
            end

          if shard_id do
            completed =
              case item do
                %{"completed" => %{"BOOL" => true}} -> true
                %{"completed" => true} -> true
                _ -> false
              end

            has_lease_owner =
              case item do
                %{"lease_owner" => %{"S" => owner}} -> owner != nil && owner != ""
                %{"lease_owner" => owner} when is_binary(owner) -> owner != ""
                _ -> false
              end

            # Calculate appropriate lease_status
            lease_status =
              cond do
                completed -> "COMPLETED"
                has_lease_owner -> "LEASED"
                true -> "AVAILABLE"
              end

            # Update the item
            update_opt = [
              expression_attribute_values: %{
                :ls => lease_status
              },
              update_expression: "SET lease_status = :ls"
            ]

            case ExAws.Dynamo.update_item(app_name, %{"shard_id" => shard_id}, update_opt)
                 |> ExAws.request() do
              {:ok, _} ->
                Logger.debug("Updated lease_status for shard #{shard_id} to #{lease_status}")

              {:error, error} ->
                Logger.error(
                  "Failed to update lease_status for shard #{shard_id}: #{inspect(error)}"
                )
            end
          end
        end)

        if items_to_update_count > 0 do
          Logger.info("Completed lease_status field migration for #{items_to_update_count} records")
        end

      {:error, error} ->
        Logger.error("Failed to scan table for lease_status migration: #{inspect(error)}")
    end

    :ok
  end

  # Ensure GSI exists or update table to add it
  defp ensure_gsi_exists(app_name) do
    case ExAws.Dynamo.describe_table(app_name) |> ExAws.request() do
      {:ok, %{"Table" => %{"GlobalSecondaryIndexes" => gsis}}} ->
        if Enum.any?(gsis, fn gsi -> gsi["IndexName"] == "LeaseStatusIndex" end) do
          Logger.info("GSI LeaseStatusIndex already exists for table #{app_name}")
          :ok
        else
          Logger.warning("GSI LeaseStatusIndex not found for table #{app_name}, adding it")
          add_gsi_to_table(app_name)
        end

      {:ok, %{"Table" => _}} ->
        Logger.warning("No GSIs found for table #{app_name}, adding LeaseStatusIndex")
        add_gsi_to_table(app_name)

      {:error, error} ->
        Logger.error("Error checking GSI existence: #{inspect(error)}")
        {:error, error}
    end
  end

  # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GlobalSecondaryIndexUpdate.html
  defp add_gsi_to_table(app_name) do
    update_request =
      ExAws.Dynamo.update_table(app_name,
        attribute_definitions: [
          %{attribute_name: "shard_id", attribute_type: "S"},
          %{attribute_name: "lease_status", attribute_type: "S"},
          %{attribute_name: "lease_owner", attribute_type: "S"}
        ],
        global_secondary_index_updates: [
          %{
            create: %{
              index_name: "LeaseStatusIndex",
              key_schema: [
                %{attribute_name: "lease_status", key_type: "HASH"},
                %{attribute_name: "lease_owner", key_type: "RANGE"}
              ],
              projection: %{
                projection_type: "ALL"
              },
              provisioned_throughput: %{
                read_capacity_units: 10,
                write_capacity_units: 10
              }
            }
          }
        ]
      )

    case ExAws.request(update_request) do
      {:ok, _} ->
        Logger.info("Successfully requested GSI creation for table #{app_name}")
        wait_for_gsi_active(app_name)

      {:error, {"ValidationException", "One or more parameter values were invalid: " <> _ = msg}} ->
        if String.contains?(msg, "already exist") do
          Logger.info("GSI attributes already exist for table #{app_name}")
          :ok
        else
          Logger.error("Failed to add GSI: #{msg}")
          {:error, :gsi_creation_failed}
        end

      {:error, error} ->
        Logger.error("Failed to add GSI: #{inspect(error)}")
        {:error, error}
    end
  end

  defp wait_for_gsi_active(app_name, retries \\ 10, delay \\ 2000) do
    if retries <= 0 do
      Logger.error("Timed out waiting for GSI to become active")
      {:error, :timeout}
    else
      case ExAws.Dynamo.describe_table(app_name) |> ExAws.request() do
        {:ok, %{"Table" => %{"GlobalSecondaryIndexes" => gsis}}} ->
          gsi = Enum.find(gsis, fn gsi -> gsi["IndexName"] == "LeaseStatusIndex" end)

          case gsi do
            %{"IndexStatus" => "ACTIVE"} ->
              Logger.info("GSI LeaseStatusIndex is now active")
              :ok

            %{"IndexStatus" => status} ->
              Logger.info("GSI LeaseStatusIndex status: #{status}, waiting...")
              Process.sleep(delay)
              wait_for_gsi_active(app_name, retries - 1, delay)

            nil ->
              Logger.error("GSI LeaseStatusIndex not found")
              {:error, :gsi_not_found}
          end

        {:error, error} ->
          Logger.error("Error checking GSI status: #{inspect(error)}")
          {:error, error}
      end
    end
  end
end
