defmodule Cassandra.Session do
  use GenServer

  require Logger

  alias Cassandra.{Cluster, Host, LoadBalancing}
  alias Cassandra.Session.Worker

  @defaults [
    reconnection_policy: Cassandra.Reconnection.Exponential,
    reconnection_args: [],
  ]

  ### Client API ###

  def start_link(cluster, options \\ []) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, [cluster, options], [name: name])
  end

  def notify(session, message) do
    GenServer.cast(session, {:notify, message})
  end

  ### GenServer Callbacks ###

  def init([cluster, options]) do
    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.put(:session, self)

    Cluster.register(cluster, self)

    send self, :connect

    state = %{
      cluster: cluster,
      options: options,
      connections: %{},
      balancer: %Cassandra.LoadBalancing.RoundRobin{num_connections: 2},
    }

    {:ok, state}
  end

  def handle_call(:state, _, state) do
    {:reply, state, state}
  end

  def handle_call({:send, request}, from, %{connections: connections, balancer: balancer} = state) do
    case CQL.encode(request) do
      :error ->
        {:reply, {:error, :encode_error}, state}
      encoded ->
        selected_connections =
          connections
          |> Enum.filter(&open?/1)
          |> LoadBalancing.select(balancer, request)
          |> Enum.map(&elem(&1, 0))

        Task.start(Worker, :send_request, [from, selected_connections, request, encoded, &retry?/1])

        {:noreply, state}
    end
  end

  def handle_cast({:notify, {change, {host, conn}}}, %{connections: conns} = state) do
    connections = case change do
      :connection_opened ->
        Map.put(conns, conn, {host, :open})

      :connection_closed ->
        Map.put(conns, conn, {host, :close})

      :connection_stopped ->
        Map.delete(conns, conn)

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        conns
    end
    {:noreply, %{state | connections: connections}}
  end

  def handle_cast({:notify, {change, %Host{} = host}}, %{connections: conns, balancer: balancer} = state) do
    connections = case change do
      :host_up ->
        existing = count_connections_to(conns, host)
        expected = LoadBalancing.count(balancer, host)
        n = expected - existing
        if n >= 0 do
          host
          |> start_connections(n, state.options)
          |> filter_map
          |> Map.merge(conns)
        else
          Logger.debug("#{__MODULE__} already connected to #{inspect host}")
          conns
        end

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        conns
    end
    {:noreply, %{state | connections: connections}}
  end

  def handle_info(:connect, %{balancer: balancer, options: options} = state) do
    connections =
      state.cluster
      |> Cluster.hosts
      |> Map.values
      |> Enum.reject(&Host.down?/1)
      |> Enum.flat_map(&start_connections(&1, LoadBalancing.count(balancer, &1), options))
      |> filter_map

    {:noreply, %{state | connections: connections}}
  end

  def handle_info({:DOWN, _ref, :process, conn, _reason}, %{connections: connections} = state) do
    Logger.warn("#{__MODULE__} connection lost")
    connections = Map.delete(connections, conn)
    {:noreply, %{state | connections: connections}}
  end

  ### Helpers ###

  defp start_connections(host, n, options) when is_integer(n) and n > 0 do
    Enum.map(1..n, fn _ -> start_connection(host, options) end)
  end

  defp start_connections(_, _, _), do: []

  defp start_connection(host, options) do
    result =
      options
      |> Keyword.put(:host, host)
      |> Cassandra.Connection.start

    {result, host}
  end

  defp started?({{:ok, _}, _}), do: true
  defp started?(_), do: false

  defp drop_ok({{:ok, conn}, host}), do: {conn, host}

  defp status_key_value({conn, host}, status), do: {conn, {host, status}}

  defp status({_, {_, status}}), do: status

  defp open?(kv), do: status(kv) == :open

  defp key({k, _}), do: k

  defp connections_to(conns, host) do
    Enum.filter(conns, fn {_, {h, _}} -> h.id == host.id end)
  end

  defp count_connections_to(conns, host) do
    conns
    |> connections_to(host)
    |> Enum.count
  end

  defp filter_map(connections) do
    connections = Enum.filter_map(connections, &started?/1, &drop_ok/1)

    connections
    |> Enum.map(&key/1)
    |> Enum.each(&Process.monitor(&1))

    connections
    |> Enum.map(&status_key_value(&1, :close))
    |> Enum.into(%{})
  end

  defp retry?(_request), do: true
end
