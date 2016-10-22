defmodule Cassandra.Cluster.Registery do
  require Logger

  alias Cassandra.{Host, Session}

  def host_found(data, {ip, _}, hosts, sessions), do: host_found(data, ip, hosts, sessions)
  def host_found(data, ip, hosts, sessions) do
    case Host.new(data, :up) do
      nil ->
        Logger.warn("#{__MODULE__} ignoring found host due to missing data #{inspect {ip, data}}")
        hosts
      host ->
        notify(sessions, :host_up, host)
        Logger.info("#{__MODULE__} new host found #{inspect ip}")
        Map.put(hosts, host.ip, host)
    end
  end

  def host_lost({ip, _}, hosts), do: host_lost(ip, hosts)
  def host_lost(ip, hosts) do
    Logger.warn("#{__MODULE__} host #{inspect ip} lost")
    Map.delete(hosts, ip)
  end

  def host_up({ip, _}, hosts, sessions), do: host_up(ip, hosts, sessions)
  def host_up(ip, hosts, sessions) do
    if Map.has_key?(hosts, ip) do
      notify(sessions, :host_up, hosts[ip])
      Logger.info("#{__MODULE__} host #{inspect ip} is up")
      put_in(hosts[ip].status, :up)
    else
      Logger.warn("#{__MODULE__} ignoring unkown host up #{inspect ip}")
      hosts
    end
  end

  def host_down({ip, _}, hosts, sessions), do: host_down(ip, hosts, sessions)
  def host_down(ip, hosts, sessions) do
    if Map.has_key?(hosts, ip) do
      notify(sessions, :host_down, hosts[ip])
      Logger.info("#{__MODULE__} host #{inspect ip} is down")
      put_in(hosts[ip].status, :down)
    else
      Logger.warn("#{__MODULE__} ignoring unkown host down #{inspect ip}")
      hosts
    end
  end

  defp notify(sessions, change, host) do
    for session <- sessions do
      Session.notify(session, {change, host.id})
    end
  end
end
