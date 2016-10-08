defmodule CQL do
  def decode(buffer) do
    {frame, rest} = CQL.Frame.decode(buffer)
    {decode_body(frame), rest}
  end

  def decode_body(nil), do: nil

  def decode_body(%CQL.Frame{opration: opration, body: body} = frame) do
    body = case opration do
      :ERROR     -> CQL.Error.decode(body)
      :READY     -> CQL.Ready.decode(body)
      :RESULT    -> CQL.Result.decode(body)
      :SUPPORTED -> CQL.Supported.decode(body)
      :EVENT     -> CQL.Event.decode(body)
      _          -> body
    end

    %CQL.Frame{frame | body: body}
  end

  def encode(request, stream \\ 0) do
    frame = CQL.Request.frame(request)
    CQL.Frame.encode(%CQL.Frame{frame | stream: stream})
  end
end
