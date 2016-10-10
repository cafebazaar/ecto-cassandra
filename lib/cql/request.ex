defprotocol CQL.Request do
  @fallback_to_any true
  def encode(request)
end

defimpl CQL.Request, for: Any do
  def encode(_), do: :error
end
