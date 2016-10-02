defmodule CQL.Result.Void do
  defstruct []

  def decode("") do
    %__MODULE__{}
  end
end

