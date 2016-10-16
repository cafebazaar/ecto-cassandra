defmodule Cassandra.Reconnection.Exponential do
  defstruct [
    current: nil,
    initial: 500,
    multiplayer: 1.6,
    jitter: 0.2,
    max: 12000,
  ]

  defimpl Cassandra.Reconnection.Policy do
    def get(exp) do
      exp.current
    end

    def next(exp) do
      current = exp.current || exp.initial
      next = current * exp.multiplayer
      noise = (:rand.uniform - 0.5) * exp.jitter * current
      %{exp | current: round(min(next, exp.max) + noise)}
    end

    def reset(exp) do
      %{exp | current: nil}
    end
  end
end
