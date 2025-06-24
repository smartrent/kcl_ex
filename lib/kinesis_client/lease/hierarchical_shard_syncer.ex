defmodule KinesisClient.HierarchicalShardSyncer do
  @moduledoc """
  Handles hierarchical shard syncing for Kinesis streams. This module is responsible
  for discovering new shards, creating leases for them, and ensuring that all shards
  are processed in the correct order based on their parent-child relationships.

  This implementation follows the KCL 3.x architecture by managing shard hierarchy and
  ensuring parent-child relationships are respected during lease creation.
  """

  require Logger
  alias KinesisClient.Stream.AppState

  @doc """
  Synchronizes the shard hierarchy by discovering new shards and creating leases for them.

  ## Parameters
  - `stream_name`: The name of the Kinesis stream.
  - `app_name`: The application name (for lease table).
  - `initial_position`: The initial position to start processing shards (`:trim_horizon`, `:latest`, or `{:at_timestamp, timestamp}`).
  - `app_state_opts`: Options for interacting with the application state.

  ## Returns
  - `{:ok, new_leases}` if the synchronization is successful, with a list of created leases.
  - `{:error, reason}` if an error occurs.
  """
  def sync_hierarchy(
        stream_name,
        app_name
      ) do
    if KinesisClient.LeaderElection.is_leader?(app_name) do
      with {:ok, kinesis_shards} <- KinesisClient.ShardDetector.list_shards(stream_name),
           leases <- AppState.list_all_leases(app_name) do
        # Build shard graph for hierarchical analysis
        shard_graph = build_shard_graph(kinesis_shards["Shards"])

        # Build a map of lease status for quick lookup
        lease_status_map =
          build_lease_status_map(leases)

        # Identify new shards that are ready to be processed
        processable_shards =
          identify_processable_shards(shard_graph, lease_status_map, app_name)

        existing_shards_with_leases = Map.keys(build_lease_status_map(leases))

        shards_without_leases = processable_shards -- existing_shards_with_leases

        # Create leases for processable shards
        create_leases_for_shards(
          shards_without_leases,
          app_name
        )

        {:ok, %{}}
      else
        {:error, reason} -> {:error, reason}
      end
    else
      Logger.info("Skipping hierarchy sync - not leader")
      {:error, :not_leader}
    end
  end

  @doc """
  Builds a directed graph representing the shard hierarchy.
  """
  defp build_shard_graph(shard_list) do
    graph = :digraph.new([:acyclic])

    Enum.each(shard_list, fn %{"ShardId" => shard_id} = s ->
      unless :digraph.vertex(graph, shard_id) do
        :digraph.add_vertex(graph, shard_id)
      end

      case s do
        %{"ParentShardId" => parent_shard_id} when not is_nil(parent_shard_id) ->
          add_vertex(graph, parent_shard_id)
          :digraph.add_edge(graph, parent_shard_id, shard_id, "parent-child")

          case s["AdjacentParentShardId"] do
            nil ->
              :ok

            x when is_binary(x) ->
              add_vertex(graph, x)
              :digraph.add_edge(graph, x, shard_id)
          end

        _ ->
          :ok
      end
    end)

    graph
  end

  defp add_vertex(graph, shard_id) do
    unless :digraph.vertex(graph, shard_id) do
      :digraph.add_vertex(graph, shard_id)
    end
  end

  @doc """
  Lists all shard relationships in the graph, sorted by dependency order.
  """
  defp list_relationships(graph) do
    n = graph |> :digraph.vertices() |> Enum.map(fn v -> {v, :digraph.in_neighbours(graph, v)} end)
    Enum.sort(n, fn {_, n1}, {_, n2} -> length(n1) <= length(n2) end)
  end

  defp build_lease_status_map(leases) do
    Enum.reduce(leases, %{}, fn lease, acc ->
      Map.put(acc, lease.shard_id, lease.completed)
    end)
  end

  @doc """
  Identifies shards that are ready to be processed based on parent-child relationships.

  A shard is processable if:
  1. It doesn't have a lease or the lease is marked as available
  2. It has no parent shards, or all parent shards are completed
  3. It is not marked as CHILD_WAITING
  """
  defp identify_processable_shards(shard_graph, lease_status_map, app_name) do
    # List all shard relationships
    relationships = list_relationships(shard_graph)

    # Get ALL active leases across ALL nodes to ensure we have complete info on parents
    all_active_leases =
      try do
        AppState.list_active_leases(app_name)
      rescue
        e ->
          Logger.error("Error fetching all active leases: #{inspect(e)}")
          []
      end

    # Get ALL leases to check for both lease_owner and lease_status
    all_leases =
      try do
        AppState.list_all_leases(app_name)
      rescue
        e ->
          Logger.error("Error fetching all leases: #{inspect(e)}")
          []
      end

    # Create a map to identify child waiting shards by both owner and status
    child_waiting_shards =
      all_leases
      |> Enum.filter(fn lease ->
        lease.lease_owner == "CHILD_WAITING" || lease.lease_status == "CHILD_WAITING"
      end)
      |> Enum.map(fn lease -> lease.shard_id end)
      |> MapSet.new()

    # Build a set of active shard IDs (being processed by any node)
    active_shard_ids = MapSet.new(all_active_leases, fn lease -> lease.shard_id end)

    processable =
      Enum.filter(relationships, fn {shard_id, parents} ->
        # Skip any shard that is marked as CHILD_WAITING
        is_child_waiting = MapSet.member?(child_waiting_shards, shard_id)

        # Extra safety check: if this shard has parents, check their completion status
        has_active_parents =
          if parents == [] do
            # No parents, so no active parents
            false
          else
            # Check if ANY parent is still active (not completed or being processed)
            Enum.any?(parents, fn parent ->
              parent_status = Map.get(lease_status_map, parent, nil) != true
              parent_active = MapSet.member?(active_shard_ids, parent)
              parent_status || parent_active
            end)
          end

        # Check if the shard itself is not already completed
        shard_completed = Map.get(lease_status_map, shard_id, false)

        # Default result - only processable if not completed, not a CHILD_WAITING shard,
        # has no active parents, and parents are handled correctly
        not shard_completed and not is_child_waiting and not has_active_parents and
          case parents do
            # Root shards (no parents) are always processable
            [] ->
              true

            # Single parent - processable ONLY if parent is definitely completed
            # AND not being actively processed anywhere
            [parent] ->
              parent_status = Map.get(lease_status_map, parent, nil)
              parent_active = MapSet.member?(active_shard_ids, parent)
              parent_status == true && not parent_active

            # Multiple parents (merge case) - processable ONLY if ALL parents are definitely completed
            # AND none are being actively processed
            [parent1, parent2] ->
              parent1_status = Map.get(lease_status_map, parent1, nil)
              parent2_status = Map.get(lease_status_map, parent2, nil)
              parent1_active = MapSet.member?(active_shard_ids, parent1)
              parent2_active = MapSet.member?(active_shard_ids, parent2)

              parent1_status == true && parent2_status == true &&
                not parent1_active && not parent2_active

            # Any other case with multiple parents - ALL must be definitely completed AND none active
            _ ->
              active_parents =
                Enum.filter(parents, fn parent -> MapSet.member?(active_shard_ids, parent) end)

              # Don't process if any parent is still active
              if length(active_parents) > 0 do
                false
              else
                # Check completion status if no active parents
                Enum.all?(parents, fn parent ->
                  Map.get(lease_status_map, parent, nil) == true
                end)
              end
          end
      end)
      |> Enum.map(fn {shard_id, _} -> shard_id end)

    if length(processable) > 0 do
      Logger.info("Identified #{length(processable)} processable shards: #{inspect(processable)}")
    end

    processable
  end

  defp create_leases_for_shards(
         processable_shards,
         app_name
       ) do
    # Create leases for each processable shard
    Enum.map(processable_shards, fn shard_id ->
      # Create a new lease for this shard
      lease =
        %{
          shard_id: shard_id,
          shard_iterator_type: :trim_horizon,
          checkpoint: :trim_horizon,
          lease_owner: "NO_OWNER",
          lease_count: 1,
          lease_status: "AVAILABLE",
          completed: false,
          last_renewal_time: System.system_time(:millisecond)
        }

      # Create the lease and return it
      case AppState.create_lease(app_name, lease) do
        :ok ->
          Logger.info("Created lease for shard #{shard_id}")

        :already_exists ->
          Logger.info("Lease for shard #{shard_id} already exists")

        {:error, reason} ->
          Logger.error("Failed to create lease for shard #{shard_id}: #{inspect(reason)}")
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end
end
