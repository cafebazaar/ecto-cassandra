defmodule Cassandra.Connection do
  use Connection

  require Logger

  alias Cassandra.Connection.Backoff
  alias :gen_tcp, as: TCP
  alias CQL.{Frame, Startup, Ready, Options, Query, QueryParams, Error, Prepare, Execute, Register, Event}
  alias CQL.Result.{Rows, Void, Prepared}

  @defaults %{
    host: "127.0.0.1",
    port: 9042,
    timeout: 5000,
    max_attempts: :infinity,
  }

  @call_timeout :infinity

  @default_params %{
    consistency: :one,
    skip_metadata: false,
    page_size: 100,
    paging_state: nil,
    serial_consistency: nil,
    timestamp: nil,
  }

  @valid_params Map.keys(@default_params)

  # Client API

  def start_link(options \\ []) do
    Connection.start_link(__MODULE__, options)
  end

  def options(connection, timeout \\ @call_timeout) do
    Connection.call(connection, :options, timeout)
  end

  def query(connection, query, options \\ [], timeout \\ @call_timeout) do
    Connection.call(connection, {:query, query, options}, timeout)
  end

  def prepare(connection, query, timeout \\ @call_timeout) do
    Connection.call(connection, {:prepare, query}, timeout)
  end

  def execute(connection, %Prepared{} = prepared, values \\ [], options \\ [], timeout \\ @call_timeout) do
    Connection.call(connection, {:execute, prepared, values, options}, timeout)
  end

  def register(connection, types, timeout \\ @call_timeout) do
    case Connection.call(connection, {:register, List.wrap(types)}, timeout) do
      {:ok, :ready} ->
        Connection.call(connection, :event_stream)
      error ->
        error
    end
  end

  def stop(connection) do
    GenServer.stop(connection)
  end

  # Connection Callbacks

  def init(options) do
    host         = Keyword.get(options, :hostname, @defaults.host) |> to_charlist
    port         = Keyword.get(options, :port, @defaults.port)
    timeout      = Keyword.get(options, :timeout, @defaults.timeout)
    keyspace     = Keyword.get(options, :keyspace)
    max_attempts = Keyword.get(options, :max_attempts, @defaults.max_attempts)

    {:ok, manager} = GenEvent.start_link

    state = %{
      host: host,
      port: port,
      timeout: timeout,
      waiting: [],
      streams: %{},
      last_stream_id: 1,
      socket: nil,
      backoff: Backoff.next,
      keyspace: keyspace,
      event_manager: manager,
      buffer: "",
      max_attempts: max_attempts,
      attempts: 1,
    }

    {:connect, :init, state}
  end

  def connect(_info, state = %{host: host, port: port, timeout: timeout, keyspace: keyspace}) do
    with {:ok, socket} <- TCP.connect(host, port, [:binary, active: false]),
         :ok <- handshake(socket, timeout),
         :ok <- use_keyspace(socket, keyspace, timeout)
    do
      :inet.setopts(socket, [active: true])
      {:ok, stream_all(%{state | socket: socket, backoff: Backoff.next})}
    else
      :stop ->
        {:stop, :handshake_error, state}
      {:error, {:keyspace, message}} ->
        Logger.error("#{__MODULE__} #{message}")
        {:stop, :invalid_keyspace, state}
      _ ->
        {attempts, state} = get_and_update_in(state.attempts, &{&1, &1 + 1})
        if attempts < state.max_attempts do
          Logger.warn("#{__MODULE__} connection failed, retrying in #{state.backoff}ms ...")
          {backoff, state} = get_and_update_in(state.backoff, &{&1, Backoff.next(&1)})
          {:backoff, backoff, state}
        else
          Logger.warn("#{__MODULE__} connection failed after #{attempts} attempts")
          {:stop, :max_attempts, state}
        end
    end
  end

  def disconnect(info, %{socket: socket} = state) do
    :ok = TCP.close(socket)
    case info do
      {:error, :closed} ->
        Logger.error("#{__MODULE__} connection closed")
      {:error, reason} ->
        message = :inet.format_error(reason)
        Logger.error("#{__MODULE__} connection error #{message}")
    end
    next_state = %{state |
      waiting: Map.values(state.streams),
      streams: %{},
      last_stream_id: 1,
      socket: nil,
    }
    {:connect, :reconnect, next_state}
  end

  def terminate(_reason, %{socket: socket} = state) do
    state.streams
    |> Map.values
    |> Enum.concat(state.waiting)
    |> Enum.each(fn {_, from} -> Connection.reply(from, {:error, :stopped}) end)

    unless is_nil(socket), do: TCP.close(socket)
  end

  def handle_call(:options, from, state) do
    {:noreply, stream(%Options{}, from, state)}
  end

  def handle_call({:query, query, options}, from, state) do
    if String.contains?(query, "?") do
      {:reply, {:error, {:invalid, "Query string can not contain bind marker `?`, use parepare instead"}}, state}
    else
      request = %Query{
        query: query,
        params: params([], options)
      }
      {:noreply, stream(request, from, state)}
    end
  end

  def handle_call({:prepare, query}, from, state) do
    request = %Prepare{query: query}
    {:noreply, stream(request, from, state)}
  end

  def handle_call({:execute, %Prepared{} = prepared, values, options}, from, state) do
    request = %Execute{
      prepared: prepared,
      params: params(values, options)
    }
    {:noreply, stream(request, from, state)}
  end

  def handle_call({:register, types}, from, state) do
    request = %Register{types: types}
    {:noreply, stream(request, from, state)}
  end

  def handle_call(:event_stream, _from, %{event_manager: manager} = state) do
    {:reply, {:stream, GenEvent.stream(manager)}, state}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    handle_data(data, state)
  end

  def handle_info({:tcp_error, socket, reason}, %{socket: socket} = state) do
    {:disconnect, {:error, reason}, state}
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:disconnect, {:error, :closed}, state}
  end

  # Helpers

  defp handle_data(data, %{buffer: buffer} = state) do
    case CQL.decode(buffer <> data) do
      {%Frame{stream: id} = frame, buffer} ->
        next_state = case id do
          -1 -> handle_event(frame, state)
           0 -> state
           _ -> handle_response(frame, state)
        end
        handle_data(buffer, next_state)
      {nil, buffer} ->
        {:noreply, %{state | buffer: buffer}}
    end
  end

  defp handle_event(%Frame{body: %Event{} = event}, state) do
    GenEvent.ack_notify(state.event_manager, event)
    state
  end

  defp handle_response(%Frame{stream: id, body: %Rows{metadata: %{paging_state: paging}, data: data}}, state) do
    {{request, from}, next_state} = pop_in(state.streams[id])
    manager = case from do
      {:gen_event, manager} ->
        manager
      from ->
        {:ok, manager} = GenEvent.start_link
        stream = GenEvent.stream(manager)
        Connection.reply(from, {:stream, stream})
        manager
    end

    Enum.map(data, &GenEvent.ack_notify(manager, &1))
    next_request = %{request | params: %{request.params | paging_state: paging}}

    stream(next_request, {:gen_event, manager}, next_state)
  end

  defp handle_response(%Frame{stream: id, body: %Rows{data: data}}, state) do
    {{_, from}, next_state} = pop_in(state.streams[id])
    case from do
      {:gen_event, manager} ->
        Enum.map(data, &GenEvent.ack_notify(manager, &1))
        GenEvent.stop(manager)
      from ->
        Connection.reply(from, {:ok, data})
    end
    next_state
  end

  defp handle_response(%Frame{stream: 1, body: body}, state) do
    case body do
      %Error{code: code, message: message} ->
        Logger.error("#{__MODULE__} error[#{code}] #{message}")
      response ->
        Logger.info("#{__MODULE__} #{inspect response}")
    end
    state
  end

  defp handle_response(%Frame{stream: id, body: body}, state) do
    {{_, from}, next_state} = pop_in(state.streams[id])
    response = case body do
      %Error{message: message, code: code} ->
        {:error, {code, message}}
      %Ready{} ->
        {:ok, :ready}
      %Void{} ->
        {:ok, :done}
      response ->
        {:ok, response}
    end
    Connection.reply(from, response)
    next_state
  end

  defp params(values, options) do
    params =
      options
      |> Keyword.take(@valid_params)
      |> Enum.into(@default_params)
      |> Map.put(:values, values)

    struct(QueryParams, params)
  end

  defp stream_all(state) do
    Enum.reduce state.waiting, state, fn
      ({request, from}, state) -> stream(request, from, state)
    end
  end

  defp stream(request, from, %{socket: nil} = state) do
    update_in(state.waiting, &[{request, from} | &1])
  end

  defp stream(request, from, %{socket: socket, last_stream_id: id} = state) do
    id = next_stream_id(id)
    case send_to(socket, request, id) do
      :error ->
        state
      _ ->
        state
        |> Map.put(:last_stream_id, id)
        |> put_in([:streams, id], {request, from})
    end
  end

  defp send_to(socket, request, id \\ 0) do
    case CQL.encode(request, id) do
      :error ->
        Logger.error("#{__MODULE__} invalid request #{inspect request}")
        :error
      frame ->
        TCP.send(socket, frame)
    end
  end

  defp handshake(socket, timeout) do
    with :ok <- send_to(socket, %Startup{}),
         {:ok, buffer} <- TCP.recv(socket, 0, timeout),
         {%Frame{body: %Ready{}}, ""} <- CQL.decode(buffer)
      do
        :ok
    else
      %Frame{body: %Error{code: code, message: message}} ->
        Logger.error("#{__MODULE__} error[#{code}] #{message}")
        :stop
      {:error, :closed} ->
        Logger.error("#{__MODULE__} connection closed before handshake")
        :error
      {:error, reason} ->
        message = :inet.format_error(reason)
        Logger.error("#{__MODULE__} handshake error: #{message}")
        :error
      error ->
        Logger.error("#{__MODULE__} handshake error: #{inspect error}")
        :error
    end
  end

  defp use_keyspace(nil, _, _), do: :ok
  defp use_keyspace(_, nil, _), do: :ok
  defp use_keyspace(socket, keyspace, timeout) do
    with :ok <- send_to(socket, %Query{query: "USE #{keyspace};"}),
         {:ok, buffer} <- TCP.recv(socket, 0, timeout),
         {%Frame{body: %CQL.Result.SetKeyspace{}}, ""} <- CQL.decode(buffer)
    do
      :ok
    else
      {%Frame{body: %CQL.Error{code: :syntax_error}}, ""} ->
        {:error, {:keyspace, "Syntax error in keyspace name"}}
      {%Frame{body: %CQL.Error{code: :invalid, message: message}}, ""} ->
        {:error, {:keyspace, message}}
      error ->
        {:error, error}
    end
  end

  defp next_stream_id(32768), do: 2
  defp next_stream_id(n), do: n + 1
end
