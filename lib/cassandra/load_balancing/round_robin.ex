defmodule Cassandra.LoadBalancing.RoundRobin do
  defstruct [num_connections: 1]

  defimpl Cassandra.LoadBalancing.Policy do
    def select(_, connections, _) do
      Enum.shuffle(connections)
    end

    def count(balancer, _) do
      balancer.num_connections
    end
  end
end
