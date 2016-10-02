defprotocol CQL.Response do
  def decode(binary)
end
