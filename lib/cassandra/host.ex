defmodule Cassandra.Host do
  @type t :: %Cassandra.Host{}

  require Logger

  defstruct [
    :ip,
    :id,
    :data_center,
    :rack,
    :release_version,
    :schema_version,
    :tokens,
    :status,
  ]

  def new(data, status \\ nil) do
    with {:ok, ip} <- peer_ip(data),
         {:ok, host} <- from_data(data)
    do
      %{host | ip: ip, status: status}
    else
      :error -> nil
    end
  end

  def up?({_, host}), do: up?(host)
  def up?(%__MODULE__{} = host), do: host.status == :up

  def down?({_, host}), do: down?(host)
  def down?(%__MODULE__{} = host), do: host.status == :down

  def toggle(%__MODULE__{} = host, status)
  when status == :up or status == :down do
    %{host | status: status}
  end

  defp peer_ip(%{"broadcast_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(%{"rpc_address" => {0, 0, 0, 0}, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => nil, "peer" => peer}), do: {:ok, peer}
  defp peer_ip(%{"rpc_address" => ip}) when not is_nil(ip), do: {:ok, ip}
  defp peer_ip(_), do: :error

  defp from_data(%{
    "host_id" => id,
    "data_center" => data_center,
    "rack" => rack,
    "release_version" => release_version,
    "schema_version" => schema_version,
    "tokens" => tokens,
  }) do
    host = %__MODULE__{
      id: id,
      data_center: data_center,
      rack: rack,
      release_version: release_version,
      schema_version: schema_version,
      tokens: tokens,
    }
    {:ok, host}
  end
  defp from_data(_), do: :error
end
