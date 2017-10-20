defmodule EctoCassandra.INet do
  @behaviour Ecto.Type

  def type, do: :inet

  def cast({_, _, _, _} = ip), do: {:ok, ip}
  def cast({_, _, _, _, _, _, _, _} = ip), do: {:ok, ip}

  def cast(ip) when is_binary(ip) do
    cast(String.to_charlist(ip))
  end

  def cast(ip) when is_list(ip) do
    case :inet_parse.address(ip) do
      {:ok, ip} -> {:ok, ip}
      _         -> :error
    end
  end

  def cast(_), do: :error

  def load({_, _, _, _} = ip), do: {:ok, ip}
  def load({_, _, _, _, _, _, _, _} = ip), do: {:ok, ip}
  def load(_), do: :error

  def dump({_, _, _, _} = ip), do: {:ok, ip}
  def dump({_, _, _, _, _, _, _, _} = ip), do: {:ok, ip}
  def dump(_), do: :error
end
