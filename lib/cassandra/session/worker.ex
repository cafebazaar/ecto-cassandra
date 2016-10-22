defmodule Cassandra.Session.Worker do
  require Logger

  def send_request(_, _, from, [], _) do
    GenServer.reply(from, {:error, :no_more_connections})
  end

  def send_request(request, encoded, from, [conn | conns], retry?) do
    Logger.debug("#{__MODULE__} sending request on #{inspect conn}")

    result = Cassandra.Connection.send(conn, encoded)

    if Cassandra.Connection.send_fail?(result) do
      if retry?.(request) do
        send_request(request, encoded, from, conns, retry?)
      else
        reply(from, {:error, :failed_in_retry_policy})
      end
    else
      reply(from, result)
    end
  end

  defp reply(nil, _), do: :ok
  defp reply(from, reply) do
    GenServer.reply(from, reply)
  end
end
