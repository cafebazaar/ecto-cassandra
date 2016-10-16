defprotocol Cassandra.Reconnection.Policy do
  def get(state)
  def next(state)
  def reset(state)
end
