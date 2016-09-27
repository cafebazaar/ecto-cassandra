defmodule Cassandra.Protocol do
  # @behaviour DBConnection

  require Logger

  alias :gen_tcp, as: TCP
  alias Cassandra.{Frame, Request}

  def connect(_options \\ []) do
    with {:ok, port} <- TCP.connect('127.0.0.1', 9042, [:binary, active: true]),
         :ok <- starup(port),
         :ok <- receive_ready(port)
    do
      {:ok, %{port: port}}
    end
  end

  def disconnect(error, %{port: port}) do
    TCP.close(port)
  end

  defp starup(port) do
    Request.startup
    |> send_request(port)
  end

  def send_request(frame = %Frame{}, port) do
    TCP.send(port, Frame.encode(frame))
  end

  def receive_responce(port, timeout \\ 5000) do
    receive do
      {:tcp, ^port, binary} ->
        IO.inspect binary
        Frame.decode(binary)
      {:tcp_closed, ^port} ->
        Logger.warn "TCP Closed"
        {:error, "connection closed"}
      {:tcp_error, ^port, reason} ->
        Logger.error "TCP Error: #{inspect reason}"
        {:error, reason}
    after
      timeout ->
        Logger.warn "TCP response timeout"
        {:error, "timeout"}
    end
  end

  defp receive_ready(port) do
    case receive_responce(port) do
      {:ok, %Frame{opration: :READY} = frame} ->
        IO.inspect frame
        :ok
      {:ok, %Frame{opration: :ERROR} = frame} ->
        IO.inspect frame
        <<code::signed-integer-size(32), _::unsigned-integer-size(16), reason::binary>> = frame.body
        IO.puts to_string(reason)
      _ ->
        :error
    end
  end
end
