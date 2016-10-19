defmodule Cassandra.Reconnection.Constant do
  defstruct [
    current: nil,
    attempts: 0,

    initial: 500,
    jitter: 0.2,
    max: 12000,
    max_attempts: 3,
  ]

  defimpl Cassandra.Reconnection.Policy do
    def get(cons) do
      if cons.attempts < cons.max_attempts do
        cons.current
      else
        :stop
      end
    end

    def next(cons) do
      current = cons.current || cons.initial
      next = current + cons.initial
      noise = (:rand.uniform - 0.5) * cons.jitter * current
      %{cons | attempts: cons.attempts + 1, current: round(min(next, cons.max) + noise)}
    end

    def reset(cons) do
      %{cons | attempts: 0, current: nil}
    end
  end
end
