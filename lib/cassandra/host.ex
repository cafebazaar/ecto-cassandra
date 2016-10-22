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
    :connections,
    :prepared_statements,
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

  def toggle_connection(%__MODULE__{} = host, conn, state) do
    put_in(host.connections[conn], state)
  end

  def put_connections(%__MODULE__{} = host, conns, state \\ :close) do
    connections =
      conns
      |> Enum.zip(Stream.cycle([state]))
      |> Enum.into(host.connections)

    put_in(host.connections, connections)
  end

  def delete_connection(%__MODULE__{} = host, conn) do
    update_in(host.connections, &Map.delete(&1, conn))
  end

  def open?(%__MODULE__{} = host, conn) do
    host.connections[conn] == :open
  end

  def open_connections(%__MODULE__{} = host) do
    Enum.filter(host.connections, fn {_, state} -> state == :open end)
  end

  def open_connections_count(%__MODULE__{} = host) do
    host
    |> open_connections
    |> Enum.count
  end

  def put_prepared_statement(%__MODULE__{} = host, hash, prepared) do
    put_in(host.prepared_statements[hash], prepared)
  end

  def delete_prepared_statements(%__MODULE__{} = host) do
    %{host | prepared_statements: %{}}
  end

  def has_prepared?(%__MODULE__{} = host, hash) do
    Map.has_key?(host.prepared_statements, hash)
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
      connections: %{},
      prepared_statements: %{},
    }
    {:ok, host}
  end
  defp from_data(_), do: :error
end
