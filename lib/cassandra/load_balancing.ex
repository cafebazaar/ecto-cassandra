defmodule Cassandra.LoadBalancing do
  alias Cassandra.LoadBalancing.Policy

  @distances [:ignore, :local, :remote]

  defmacro distances, do: @distances

  def select(connections, balancer, request) do
    Policy.select(balancer, connections, request)
  end

  def count(balancer, host) do
    Policy.count(balancer, host)
  end
end
