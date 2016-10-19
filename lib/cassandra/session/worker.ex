defmodule Cassandra.Session.Worker do
  require Logger

  def send_request(from, [], _, _, _) do
    GenServer.reply(from, {:error, :no_more_connections})
  end

  def send_request(from, [conn | conns], request, encoded, retry?) do
    Logger.debug("#{__MODULE__} sending request on #{inspect conn}")
    result = Cassandra.Connection.send(conn, encoded)
    if Cassandra.Connection.send_fail?(result) do
      if retry?.(request) do
        send_request(from, conns, request, encoded, retry?)
      else
        GenServer.reply(from, {:error, :failed_in_retry_policy})
      end
    else
      GenServer.reply(from, result)
    end
  end
end
