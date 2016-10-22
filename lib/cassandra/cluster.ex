defmodule Cassandra.Cluster do
  use GenServer

  require Logger

  alias Cassandra.{Host, Session}
  alias Cassandra.Cluster.Registery

  @select_peers CQL.encode(%CQL.Query{query: "SELECT * FROM system.peers;"})
  @select_local CQL.encode(%CQL.Query{query: "SELECT * FROM system.local;"})
  @register_events CQL.encode(%CQL.Register{})

  ### Client API ###

  def start(contact_points \\ ["127.0.0.1"], options \\ [], opts \\ []) do
    GenServer.start(__MODULE__, [contact_points, options], opts)
  end

  def start_link(contact_points \\ ["127.0.0.1"], options \\ [], opts \\ []) do
    GenServer.start(__MODULE__, [contact_points, options], opts)
  end

  def hosts(cluster) do
    GenServer.call(cluster, :hosts)
  end

  def register(cluster, session) do
    GenServer.cast(cluster, {:register, session})
  end

  def connect_link(cluster, options \\ []) do
    Session.start_link(cluster, options)
  end

  ### GenServer Callbacks ###

  def init([contact_points, options]) do
    options = Keyword.put(options, :event_manager, self)

    with {:ok, conn, local} <- setup(contact_points, options),
         {:ok, peers} <- Cassandra.Connection.send(conn, @select_peers),
         {:ok, :ready} <- Cassandra.Connection.send(conn, @register_events)
    do
      peer_hosts =
        peers
        |> Enum.map(&Host.new/1)
        |> Enum.reject(&is_nil/1)

      local_host =
        local
        |> Host.new
        |> Host.toggle(:up)

      hosts =
        [local_host | peer_hosts]
        |> Enum.map(fn h -> {h.ip, h} end)
        |> Enum.into(%{})

      {:ok, %{
        options: options,
        name: local["cluster_name"],
        control_connection: conn,
        hosts: hosts,
        sessions: [],
      }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_cast({:notify, event}, %{hosts: hosts, sessions: sessions} = state) do
    hosts = case event_type(event) do
      {:host_found, address} ->
        address
        |> select_peer(state.control_connection)
        |> Registery.host_found(address, hosts, sessions)

      {:host_lost, address} ->
        Registery.host_lost(address, hosts)

      {:host_up, address} ->
        Registery.host_up(address, hosts, sessions)

      {:host_down, address} ->
        Registery.host_down(address, hosts)

      :not_related ->
        hosts
    end

    {:noreply, %{state | hosts: hosts}}
  end

  def handle_cast({:register, session}, %{sessions: sessions} = state) do
    {:noreply, %{state | sessions: [session | sessions]}}
  end

  def handle_call(:hosts, _from, state) do
    {:reply, state.hosts, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    Logger.warn("#{__MODULE__} control connection lost")
    contact_points = Map.keys(state.hosts)

    with {:ok, conn, _local} <- setup(contact_points, state.options),
         {:ok, :ready} <- Cassandra.Connection.send(conn, @register_events)
    do
      Logger.warn("#{__MODULE__} new control connection opened")
      {:noreply, %{state | control_connection: conn}}
    else
      {:error, reason} ->
        Logger.error("#{__MODULE__} control connection not found")
        {:stop, reason, state}
    end
  end

  ### Helpers ###

  defp setup(contact_points, options) do
    connection_options = Keyword.merge(options, [async_init: false])

    contact_points
    |> Stream.map(&start_connection(&1, connection_options))
    |> Stream.filter_map(&ok?/1, &value/1)
    |> Stream.map(&select_local/1)
    |> Stream.reject(&error?/1)
    |> Stream.filter(&bootstrapped?/1)
    |> Enum.take(1)
    |> case do
      [{conn, local}] ->
        Process.monitor(conn)
        {:ok, conn, local}
      [] ->
        {:error, :no_host_avaliable}
    end
  end

  defp start_connection({address, port}, options) do
    start_connection(address, Keyword.put(options, :port, port))
  end

  defp start_connection(address, options) do
    options
    |> Keyword.put(:host, address)
    |> Cassandra.Connection.start
  end

  defp ok?({:ok, _}), do: true
  defp ok?(_), do: false

  defp error?(:error), do: true
  defp error?(_), do: false

  defp value({_, v}), do: v

  defp bootstrapped?({_conn, local}) do
    Map.get(local, "bootstrapped") == "COMPLETED"
  end

  defp select_local(conn) do
    case Cassandra.Connection.send(conn, @select_local) do
      {:ok, [local]} -> {conn, local}
      _              -> :error
    end
  end

  defp select_peer({address, _}, conn) do
    case Cassandra.Connection.send(conn, peer_query(address)) do
      {:ok, [peer]} -> peer
      _             -> %{}
    end
  end

  defp peer_query(ip) do
    CQL.encode(%CQL.Query{query: "SELECT * FROM system.peers WHERE peer='#{ip_to_string(ip)}';"})
  end

  defp ip_to_string({_, _, _, _} = ip) do
    ip
    |> Tuple.to_list
    |> Enum.join(".")
  end

  defp ip_to_string({_, _, _, _, _, _} = ip) do
    ip
    |> Tuple.to_list
    |> Enum.map(&Integer.to_string(&1, 16))
    |> Enum.join(":")
  end

  defp event_type(%CQL.Event{type: "TOPOLOGY_CHANGE", info: %{change: "NEW_NODE", address: address}}) do
    {:host_found, address}
  end

  defp event_type(%CQL.Event{type: "TOPOLOGY_CHANGE", info: %{change: "REMOVED_NODE", address: address}}) do
    {:host_lost, address}
  end

  defp event_type(%CQL.Event{type: "STATUS_CHANGE", info: %{change: "UP", address: address}}) do
    {:host_up, address}
  end

  defp event_type(%CQL.Event{type: "STATUS_CHANGE", info: %{change: "DOWN", address: address}}) do
    {:host_down, address}
  end

  defp event_type(_) do
    :not_related
  end
end
