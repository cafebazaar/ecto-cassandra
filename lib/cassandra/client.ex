defmodule Cassandra.Client do
  use GenServer

  require Logger

  alias :gen_tcp, as: TCP

  # Client API

  def start_link(options \\ []) do
    GenServer.start_link(__MODULE__, options)
  end

  def options(client) do
    GenServer.call(client, :options)
  end

  def query(client, query, values \\ nil, options \\ []) do
    GenServer.call(client, {:query, query, values, options})
  end

  def prepare(client, query) do
    GenServer.call(client, {:prepare, query})
  end

  def execute(client, id, values \\ nil, options \\ []) do
    GenServer.call(client, {:execute, id, values, options})
  end

  def stop(client) do
    GenServer.stop(client)
  end

  # GenServer Callbacks

  def init(options) do
    host    = Keyword.get(options, :hostname, "127.0.0.1") |> to_charlist
    port    = Keyword.get(options, :port, 9042)
    timeout = Keyword.get(options, :timeout, 5000)

    with {:ok, socket} <- TCP.connect(host, port, [:binary, active: true]),
         :ok <- startup(socket),
         :ok <- receive_ready(socket)
    do
      {:ok, %{
        socket: socket,
        timeout: timeout,
        active: true,
        awaiting: %{},
        stream: 1,
      }}
    end
  end

  def handle_call(:options, from, state) do
    %CQL.Options{}
    |> stream_request(from, state)
  end

  def handle_call({:query, query, values, options}, from, state) do
    %CQL.Query{query: query, params: params(values, options)}
    |> stream_request(from, state)
  end

  def handle_call({:prepare, %CQL.Prepare{} = query}, from, state) do
    query
    |> stream_request(from, state)
  end

  def handle_call({:prepare, query}, from, state) do
    %CQL.Prepare{query: query}
    |> stream_request(from, state)
  end

  def handle_call({:execute, id, %CQL.QueryParams{} = params, options}, from, state) do
    %CQL.Execute{id: id, params: params}
    |> stream_request(from, state)
  end

  def handle_call({:execute, id, values, options}, from, state) do
    handle_call({:execute, id, params(values, options), options}, from, state)
  end

  def handle_info({:tcp, socket, buffer}, %{socket: socket} = state) do
    %CQL.Frame{stream: stream, body: body} = frame = CQL.decode(buffer)
    {from, new_state} = pop_in(state.awaiting[stream])
    GenServer.reply(from, body)
    {:noreply, new_state}
  end

  def terminate(reason, %{socket: socket, awaiting: awaiting}) do
    awaiting
    |> Enum.each(fn {_, from} -> GenServer.reply(from, {:error, reason}) end)
    TCP.close(socket)
  end

  # Helpers

  defp startup(socket) do
    %CQL.Startup{}
    |> CQL.encode
    |> send_request(socket)
  end

  defp params(values, options) do
    consistency = Keyword.get(options, :consistency, :ONE)
    %CQL.QueryParams{values: values, consistency: consistency}
  end

  defp stream_request(request, from, %{socket: socket} = state) do
    request
    |> CQL.encode(state.stream)
    |> send_request(socket)

    new_state =
      state
      |> put_in([:awaiting, state.stream], from)
      |> update_in([:stream], &(&1 + 1))
    {:noreply, new_state}
  end

  defp send_request(request, socket) do
    TCP.send(socket, request)
  end

  defp receive_responce(socket, timeout \\ 5000) do
    receive do
      {:tcp, ^socket, binary} ->
        CQL.decode(binary)
      {:tcp_closed, ^socket} ->
        Logger.warn "TCP Closed"
        {:error, "connection closed"}
      {:tcp_error, ^socket, reason} ->
        Logger.error "TCP Error: #{inspect reason}"
        {:error, reason}
    after
      timeout ->
        Logger.warn "TCP response timeout"
        {:error, "timeout"}
    end
  end

  defp receive_ready(socket) do
    case receive_responce(socket) do
      %CQL.Frame{body: %CQL.Ready{}} ->
        :ok
      %CQL.Frame{body: %CQL.Error{code: code, message: message}} ->
        Logger.error "Connction error: [#{inspect code}] #{inspect message}"
        :error
      _ ->
        :error
    end
  end
end
