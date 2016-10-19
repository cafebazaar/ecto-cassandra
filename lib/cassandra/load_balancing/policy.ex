defprotocol Cassandra.LoadBalancing.Policy do
  def select(balancer, connections, request)
  def count(balancer, host)
end
