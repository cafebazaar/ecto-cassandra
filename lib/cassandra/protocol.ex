defmodule Cassandra.Protocol do
  # @behaviour DBConnection

  require Logger

  alias :gen_tcp, as: TCP
  alias :inet, as: INet
  alias CQL.{Ready, Error, Startup, Query}

  def connect(options) do
    host = options |> Keyword.get(:hostname, "127.0.0.1") |> to_charlist
    port = options[:port] || 9042
    timeout = options[:timeout] || 5000

    with {:ok, socket} <- TCP.connect(host, port, [:binary, active: true]),
         :ok <- startup(socket),
         :ok <- receive_ready(socket)
    do
      {:ok, %{
        socket: socket,
        host: host,
        port: port,
        timeout: timeout,
        active: true,
      }}
    end
  end

  def disconnect(error, %{socket: socket}) do
    Logger.warn "Disconnecting: #{inspect error}"
    TCP.close(socket)
  end

  def checkin(%{socket: socket, active: false} = state) do
    setopts(socket, [active: true])
    {:ok, %{state | active: true}}
  end

  def checkin(state) do
    {:disconnect, ArgumentError.exception("Bad status"), state}
  end

  def checkout(%{socket: socket, active: true} = state) do
    setopts(socket, [active: false])
    {:ok, %{state | active: false}}
  end

  def checkout(state) do
    {:disconnect, ArgumentError.exception("Bad status"), state}
  end

  def handle_begin(_options, state) do
    {:error, RuntimeError.exception("not supported"), state}
  end

  def handle_rollback(_options, state) do
    {:error, RuntimeError.exception("not supported"), state}
  end

  def ping(state) do
    {:ok, state}
  end

  defp startup(socket) do
    options = %{"CQL_VERSION" => "3.0.0"}
    send_request(%Startup{options: options}, socket)
  end

  def run(socket, query) when is_bitstring(query) do
    run(socket, %Query{query: query})
  end

  def run(socket, request) do
    :ok = send_request(request, socket)
    receive_responce(socket)
  end

  def send_request(request, socket) do
    TCP.send(socket, CQL.encode(request))
  end

  def receive_responce(socket, timeout \\ 5000) do
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
      %Ready{} ->
        :ok
      %Error{code: code, message: message} ->
        Logger.error "Connction error: [#{inspect code}] #{inspect message}"
        :error
      _ ->
        :error
    end
  end

  def handle_info(message, options, state) do
    IO.inspect {message, options, state}
  end

  defp setopts(socket, opts) do
    INet.setopts(socket, opts)
  end
end
