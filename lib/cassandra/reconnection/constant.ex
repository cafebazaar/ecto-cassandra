defmodule Cassandra.Reconnection.Constant do
  defstruct [
    current: nil,
    initial: 500,
    jitter: 0.2,
    max: 12000,
  ]

  defimpl Cassandra.Reconnection.Policy do
    def get(cons) do
      cons.current
    end

    def next(cons) do
      current = cons.current || cons.initial
      next = current + cons.initial
      noise = (:rand.uniform - 0.5) * cons.jitter * current
      %{cons | current: round(min(next, cons.max) + noise)}
    end

    def reset(cons) do
      %{cons | current: nil}
    end
  end
end
