defmodule CQL do
  def decode(binary) do
    frame = CQL.Frame.decode(binary)

    body = case frame.opration do
      :ERROR     -> CQL.Error.decode(frame.body)
      :READY     -> CQL.Ready.decode(frame.body)
      :RESULT    -> CQL.Result.decode(frame.body)
      :SUPPORTED -> CQL.Supported.decode(frame.body)
      :EVENT     -> CQL.Event.decode(frame.body)
      _          -> frame.body
    end

    %CQL.Frame{frame | body: body}
  end

  def encode(request, stream \\ 0) do
    frame = CQL.Request.frame(request)
    CQL.Frame.encode(%CQL.Frame{frame | stream: stream})
  end
end
