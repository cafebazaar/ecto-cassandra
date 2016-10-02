defmodule CQL do
  def decode(binary) do
    frame = CQL.Frame.decode(binary)

    case frame.opration do
      :ERROR     -> CQL.Error.decode(frame)
      :READY     -> CQL.Ready.decode(frame)
      :RESULT    -> CQL.Result.decode(frame)
      :SUPPORTED -> CQL.Supported.decode(frame)
      _          -> frame
    end
  end

  def encode(request) do
    request
    |> CQL.Request.frame
    |> CQL.Frame.encode
  end
end
