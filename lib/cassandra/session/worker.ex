defmodule Cassandra.Session.Worker do
  require Logger

  def send_request(_, from, [], _) do
    GenServer.reply(from, {:error, :no_more_connections})
  end

  def send_request(request, from, [conn | conns], retry?) do
    Logger.debug("#{__MODULE__} sending request on #{inspect conn}")

    result = Cassandra.Connection.send(conn, request)

    if Cassandra.Connection.send_fail?(result) do
      if retry?.(request) do
        send_request(request, from, conns, retry?)
      else
        GenServer.reply(from, {:error, :failed_in_retry_policy})
      end
    else
      GenServer.reply(from, result)
    end
  end
end
