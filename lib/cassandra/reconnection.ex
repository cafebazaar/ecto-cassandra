defmodule Cassandra.Reconnection do
  alias Cassandra.Reconnection.Policy

  def start_link(module, args) do
    Agent.start_link(fn -> struct(module, args) end)
  end

  def next(agent) when is_pid(agent) do
    Agent.get_and_update(agent, &get_and_update/1)
  end

  def get(agent) when is_pid(agent) do
    Agent.get(agent, Policy, :get, [])
  end

  def reset(agent) when is_pid(agent) do
    Agent.update(agent, Policy, :reset, [])
  end

  defp get_and_update(state) do
    state = Policy.next(state)
    {Policy.get(state), state}
  end
end

