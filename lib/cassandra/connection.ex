defmodule Cassandra.Connection do
  use Connection

  require Logger

  alias :gen_tcp, as: TCP
  alias CQL.{Frame, Startup, Ready, Event, Error}
  alias CQL.Result.{Rows, Void}
  alias Cassandra.{Session, Reconnection, Host}

  @default_options [
    host: "127.0.0.1",
    port: 9042,
    connect_timeout: 5000,
    timeout: :infinity,
    reconnection_policy: Cassandra.Reconnection.Exponential,
    reconnection_args: [],
    session: nil,
    event_manager: nil,
    async_init: true,
  ]

  @call_timeout 5000

  # Client API

  def start(options \\ []) do
    Connection.start(__MODULE__, options)
  end

  def start_link(options \\ []) do
    Connection.start_link(__MODULE__, options)
  end

  def send(connection, request, timeout \\ @call_timeout) do
    Connection.call(connection, {:send_request, request}, timeout)
  end

  def send_async(connection, request) do
    send_async(connection, request, {self, make_ref})
  end

  def send_async(connection, request, {pid, ref}) do
    Connection.cast(connection, {:send_request, request, {pid, ref}})
    ref
  end

  def send_fail?({:ok, _}), do: false
  def send_fail?({:error, {_, _}}), do: false # is cql error
  def send_fail?({:error, _}), do: true # is connection error

  def stop(connection) do
    GenServer.stop(connection)
  end

  # Connection Callbacks

  def init(options) do
    options = Keyword.merge(@default_options, options)

    {:ok, reconnection} =
      options
      |> Keyword.take([:reconnection_policy, :reconnection_args])
      |> Reconnection.start_link

    host = case options[:host] do
      address when is_bitstring(address) -> to_charlist(address)
      inet -> inet
    end

    state =
      options
      |> Keyword.take([:port, :connect_timeout, :timeout, :session, :event_manager])
      |> Enum.into(%{
        host: host,
        streams: %{},
        last_stream_id: 1,
        socket: nil,
        buffer: "",
        reconnection: reconnection,
      })

    if options[:async_init] == true do
      {:connect, :init, state}
    else
      with {:ok, socket} <- startup(state.host, state.port, state.connect_timeout, state.timeout) do
        after_connect(socket, state)
      else
        _ -> {:stop, :connection_failed}
      end
    end
  end

  def connect(_info, state = %{host: host, port: port, connect_timeout: connect_timeout, timeout: timeout}) do
    with {:ok, socket} <- startup(host, port, connect_timeout, timeout) do
      after_connect(socket, state)
    else
      :stop ->
        {:stop, {:shutdown, :handshake_error}, state}
      _ ->
        case Reconnection.next(state.reconnection) do
          :stop ->
            Logger.error("#{__MODULE__} connection failed after max attempts")
            {:stop, {:shutdown, :max_attempts}, state}
          backoff ->
            Logger.warn("#{__MODULE__} connection failed, retrying in #{backoff}ms ...")
            {:backoff, backoff, state}
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
      :timeout ->
        Logger.error("#{__MODULE__} connection timeout")
    end

    notify(state, :connection_closed)
    reply_all(state, {:error, :closed})

    next_state = %{
      state |
      streams: %{},
      last_stream_id: 1,
      socket: nil,
    }

    {:connect, :reconnect, next_state}
  end

  def terminate(reason, state) do
    reply_all(state, {:error, :closed})
    notify(state, :connection_stopped)
    reason
  end

  def handle_cast({:send_request, request, from}, state) do
    case send_request(request, from, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        {:disconnect, {:error, reason}}
    end
  end

  def handle_call({:send_request, _}, _, %{socket: nil} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:send_request, request}, from, state) do
    case send_request(request, from, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        {:disconnect, {:error, reason}}
    end
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

  def handle_info(:timeout, state) do
    {:disconnect, :timeout, state}
  end

  # Helpers

  defp handle_data(data, %{buffer: buffer} = state) do
    case CQL.decode(buffer <> data) do
      {%Frame{stream: id} = frame, rest} ->
        result = case id do
          -1 -> handle_event(frame, state)
           0 -> {:ok, state}
           _ -> handle_response(frame, state)
        end
        case result do
          {:ok, next_state} ->
            handle_data(rest, %{next_state | buffer: ""})
          {:error, reason} ->
            {:disconnect, {:error, reason}, %{state | buffer: ""}}
        end
      {nil, buffer} ->
        {:noreply, %{state | buffer: buffer}}
    end
  end

  defp handle_event(%Frame{body: %Event{} = event}, %{event_manager: nil} = state) do
    Logger.warn("#{__MODULE__} unhandled CQL event (missing event_manager) #{inspect event}")
    {:ok, state}
  end

  defp handle_event(%Frame{body: %Event{} = event}, %{event_manager: pid} = state) do
    Logger.debug("#{__MODULE__} got event #{inspect event}")
    GenServer.cast(pid, {:notify, event})
    {:ok, state}
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

    send_request(next_request, {:gen_event, manager}, next_state)
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
    {:ok, next_state}
  end

  defp handle_response(%Frame{stream: 1, body: body}, state) do
    case body do
      %Error{code: code, message: message} ->
        Logger.error("#{__MODULE__} error[#{code}] #{message}")
      response ->
        Logger.info("#{__MODULE__} #{inspect response}")
    end
    {:ok, state}
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
    {:ok, next_state}
  end

  # defp params(values, options) do
  #   params =
  #     options
  #     |> Keyword.take(@valid_params)
  #     |> Enum.into(@default_params)
  #     |> Map.put(:values, values)

  #   struct(QueryParams, params)
  # end

  # defp stream_all(state) do
  #   Enum.reduce state.waiting, state, fn
  #     ({request, from}, state) -> stream(request, from, state)
  #   end
  # end

  defp send_request(_, from, %{socket: nil} = state) do
    Connection.reply(from, {:error, :not_connected})
    {:ok, state}
  end

  defp send_request(request, from, %{socket: socket, last_stream_id: id} = state) do
    id = next_stream_id(id)
    case send_to(socket, request, id) do
      :ok ->
        next_state =
          state
          |> Map.put(:last_stream_id, id)
          |> put_in([:streams, id], {request, from})

        {:ok, next_state}

      {:error, :invalid} ->
        Logger.error("#{__MODULE__} invalid request #{inspect request}")
        Connection.reply(from, {:error, :invalid})
        {:ok, state}

      {:error, :timeout} ->
        Logger.error("#{__MODULE__} TCP send timeout")
        Connection.reply(from, {:error, :timeout})
        {:error, :timeout}

      {:error, reason} ->
        message = :inet.format_error(reason)
        Logger.error("#{__MODULE__} TCP error #{message}")
        Connection.reply(from, {:error, message})
        {:error, reason}
    end
  end

  defp reply_all(%{streams: streams}, message) do
    streams
    |> Map.values
    |> Enum.each(fn {_, from} -> Connection.reply(from, message) end)
  end

  defp startup(%Host{ip: ip}, port, connect_timeout, timeout) do
    startup(ip, port, connect_timeout, timeout)
  end

  defp startup(host, port, connect_timeout, timeout) do
    with {:ok, socket} <- TCP.connect(host, port, [:binary, active: false], connect_timeout),
         :ok <- handshake(socket, timeout)
    do
      {:ok, socket}
    end
  end

  defp after_connect(socket, state) do
    :inet.setopts(socket, [
      active: true,
      send_timeout: state.timeout,
      send_timeout_close: true,
    ])

    notify(state, :connection_opened)

    Reconnection.reset(state.reconnection)

    {:ok, %{state | socket: socket}}
  end

  defp send_to(socket, request) do
    TCP.send(socket, request)
  end

  defp send_to(socket, request, id) do
    case CQL.set_stream_id(request, id) do
      {:ok, request_with_id} ->
        send_to(socket, request_with_id)
      :error ->
        {:error, :invalid}
    end
  end

  defp handshake(socket, timeout) do
    with :ok <- send_to(socket, CQL.encode(%Startup{})),
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

  defp notify(%{session: nil}, _), do: :ok
  defp notify(%{session: session, host: host}, message) do
    Session.notify(session, {message, {host, self}})
  end

  defp next_stream_id(32768), do: 2
  defp next_stream_id(n), do: n + 1
end
