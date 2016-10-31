defmodule Cassandra.Session do
  use GenServer

  require Logger

  alias Cassandra.{Cluster, Host, LoadBalancing, Reconnection}
  alias Cassandra.Session.Worker

  @defaults [
    reconnection_policy: Reconnection.Exponential,
    reconnection_args: [],
  ]

  @default_balancer %LoadBalancing.RoundRobin{}

  ### Client API ###

  def start_link(cluster, options \\ [], gen_options \\ []) do
    GenServer.start_link(__MODULE__, [cluster, options], gen_options)
  end

  def notify(session, message) do
    GenServer.cast(session, {:notify, message})
  end

  def execute(session, statement, options) do
    GenServer.call(session, {:execute, statement, options})
  end

  def prepare(session, statement) do
    GenServer.call(session, {:prepare, statement})
  end

  def send(session, request) do
    GenServer.call(session, {:send, request})
  end

  ### GenServer Callbacks ###

  def init([cluster, options]) do
    options =
      @defaults
      |> Keyword.merge(options)
      |> Keyword.put(:session, self)

    {balancer, options} = Keyword.pop(options, :balancer, @default_balancer)

    Cluster.register(cluster, self)

    Kernel.send(self, :connect)

    state = %{
      cluster: cluster,
      options: options,
      balancer: balancer,
      retry: &retry?/1,
      hosts: %{},
      requests: [],
      statements: %{},
      refs: %{},
    }

    {:ok, state}
  end

  def handle_call(:state, _, state) do
    {:reply, state, state}
  end

  def handle_call({:send, request}, from, state) do
    handle_send(request, from, state)
  end

  def handle_call({:prepare, statement}, from, state) do
    prepare = %CQL.Prepare{query: statement}
    send_through_self(prepare, {from, statement}, state)
  end

  def handle_call({:execute, statement, options}, from, state)
  when is_bitstring(statement)
  do
    if has_values?(options) do
      prepare_and_execute(statement, options, from, state)
    else
      query = %CQL.Query{query: statement, params: struct(CQL.QueryParams, options)}
      handle_send(query, from, state)
    end
  end

  def handle_cast({:notify, {change, {id, conn}}}, %{hosts: hosts} = state) do
    hosts = case change do
      :connection_opened ->
        update_in(hosts[id], &Host.toggle_connection(&1, conn, :open))

      :connection_closed ->
        update_in(hosts[id], &Host.toggle_connection(&1, conn, :close))

      :connection_stopped ->
        update_in(hosts[id], &Host.delete_connection(&1, conn))

      {:prepared, hash, prepared} ->
        update_in(hosts[id], &Host.put_prepared_statement(&1, hash, prepared))

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        hosts
    end

    state = %{state | hosts: hosts}

    state = if change == :connection_opened do
      send_requests(state)
    else
      state
    end

    {:noreply, state}
  end

  def handle_cast({:notify, {change, id}}, %{hosts: hosts, balancer: balancer} = state) do
    hosts = case change do
      :host_up ->
        host = hosts[id]
        existing = Enum.count(host.connections)
        expected = LoadBalancing.count(balancer, host)
        n = expected - existing
        if n >= 0 do
          updated_hosts =
            host
            |> start_connections(n, state.options)
            |> filter_map

          Map.merge(hosts, updated_hosts)
        else
          Logger.debug("#{__MODULE__} already connected to #{inspect id}")
          hosts
        end

      :host_down ->
        update_in(hosts[id], &Host.delete_prepared_statements(&1))

      other ->
        Logger.warn("#{__MODULE__} unhandled notify #{inspect other}")
        hosts
    end
    {:noreply, %{state | hosts: hosts}}
  end

  def handle_info(:connect, %{balancer: balancer, options: options} = state) do
    hosts =
      state.cluster
      |> Cluster.hosts
      |> Map.values
      |> Enum.reject(&Host.down?/1)
      |> Enum.flat_map(&start_connections(&1, LoadBalancing.count(balancer, &1), options))
      |> filter_map

    {:noreply, %{state | hosts: hosts}}
  end

  def handle_info({:DOWN, _ref, :process, conn, _reason}, %{hosts: hosts} = state) do
    Logger.warn("#{__MODULE__} connection lost")
    hosts =
      hosts
      |> Enum.map(fn {id, host} -> {id, Host.delete_connection(host, conn)} end)
      |> Enum.into(%{})
    {:noreply, %{state | hosts: hosts}}
  end

  def handle_info({ref, result}, state) do
    case pop_in(state.refs[ref]) do
      {nil, state} ->
        {:noreply, state}

      {{from, statement}, state} ->
        reply = case result do
          {:ok, _}        -> {:ok, statement}
          {:error, error} -> error
        end
        GenServer.reply(from, reply)
        {:noreply, state}

      {{from, prepare, hash, options}, state} ->
        case result do
          {:ok, _} ->
            execute(prepare, hash, options, from, state)
          {:error, error} ->
            GenServer.reply(from, error)
            {:noreply, state}
        end
    end
  end

  ### Helpers ###

  defp send_through_self(request, data, state) do
    ref = make_ref
    next_state = put_in(state.refs[ref], data)
    handle_send(request, {self, ref}, next_state)
  end

  defp handle_send(request, from, %{hosts: hosts, balancer: balancer, retry: retry} = state) do
    if open_connections_count(hosts) < 1 do
      {:noreply, %{state | requests: [{from, request} | state.requests]}}
    else
      start_task({from, request}, hosts, balancer, retry)
      {:noreply, state}
    end
  end

  defp prepare_and_execute(statement, options, from, state) when is_bitstring(statement) do
    prepare = %CQL.Prepare{query: statement}
    encoded = CQL.encode(prepare)
    hash = :crypto.hash(:md5, encoded)
    execute(prepare, hash, options, from, state)
  end

  defp execute(prepare, hash, options, from, state) do
    preferred_hosts =
      state.hosts
      |> Map.values
      |> Enum.filter(&Host.has_prepared?(&1, hash))

    if open_connections_count(preferred_hosts) == 0 do
      send_through_self(prepare, {from, prepare, hash, options}, state)
    else
      host = hd(preferred_hosts)
      prepared = host.prepared_statements[hash]
      execute = %CQL.Execute{prepared: prepared, params: struct(CQL.QueryParams, options)}
      start_task({from, execute}, preferred_hosts, state.balancer, state.retry)
      {:noreply, state}
    end
  end

  defp has_values?(options) do
    count =
      options
      |> Keyword.get(:values, [])
      |> Enum.count

    count > 0
  end

  defp select(request, hosts, balancer) when is_map(hosts) do
    select(request, Map.values(hosts), balancer)
  end

  defp select(request, hosts, balancer) do
    hosts
    |> LoadBalancing.select(balancer, request)
    |> Enum.map(&key/1)
  end

  defp start_task({from, request}, hosts, balancer, retry) do
    conns = select(request, hosts, balancer)
    Task.start(Worker, :send_request, [request, from, conns, retry])
  end

  defp send_requests(%{hosts: hosts, balancer: balancer, retry: retry} = state) do
    state.requests
    |> Enum.reverse
    |> Enum.each(&start_task(&1, hosts, balancer, retry))

    %{state | requests: []}
  end

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

  defp to_host_conn_pair({{:ok, conn}, host}), do: {host, conn}

  defp key({k, _}), do: k

  defp value({_, v}), do: v

  defp open_connections_count(hosts) when is_map(hosts) do
    hosts
    |> Map.values
    |> open_connections_count
  end

  defp open_connections_count(hosts) do
    hosts
    |> Enum.map(&Host.open_connections_count(&1))
    |> Enum.reduce(0, &(&1 + &2))
  end

  defp filter_map(connections) do
    connections = Enum.filter_map(connections, &started?/1, &to_host_conn_pair/1)

    connections
    |> Enum.map(&value/1)
    |> Enum.each(&Process.monitor/1)

    connections
    |> Enum.group_by(&key/1, &value/1)
    |> Enum.map(fn {host, conns} -> {host.id, Host.put_connections(host, conns, :close)} end)
    |> Enum.into(%{})
  end

  defp retry?(_request), do: true
end
