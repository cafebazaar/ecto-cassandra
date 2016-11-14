defmodule EctoCassandra.INet do
  @behaviour Ecto.Type

  def type, do: :inet

  def cast({_, _, _, _} = ip), do: {:ok, ip}
  def cast({_, _, _, _, _, _} = ip), do: {:ok, ip}

  def cast(ip) when is_binary(ip) do
    case String.split(ip, ".") do
      list when is_list(list) and length(list) == 4 ->
        {:ok, List.to_tuple(list)}
      _ ->
        case String.split(ip, ":") do
          list when is_list(list) and length(list) == 6 ->
            {:ok, List.to_tuple(list)}
          _ ->
            :error
        end
    end
  end

  def cast(_), do: :error

  def load({_, _, _, _} = ip), do: {:ok, ip}
  def load({_, _, _, _, _, _} = ip), do: {:ok, ip}
  def load(_), do: :error

  def dump({_, _, _, _} = ip), do: {:ok, ip}
  def dump({_, _, _, _, _, _} = ip), do: {:ok, ip}
  def dump(_), do: :error
end
