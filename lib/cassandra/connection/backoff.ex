defmodule Cassandra.Connection.Backoff do
  @init 500
  @mult 1.6
  @jitt 0.2
  @max  12000

  def init, do: @init

  def next(current) do
    next = current * @mult
    jitt = (:rand.uniform - 0.5) * @jitt * current
    round(min(next, @max) + jitt)
  end
end
