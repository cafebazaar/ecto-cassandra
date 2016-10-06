defmodule CQL.DataTypesTest do
  use ExUnit.Case
  use ExCheck

  import CQL.DataTypes

  property "ascii" do
    for_all bin in binary do
      bin == bin |> encode(:ascii) |> decode(:ascii)
    end
  end

  property "bigint" do
    for_all n in int do
      n == n |> encode(:bigint) |> has_size(8) |> decode(:bigint)
    end
  end

  property "blob" do
    for_all bin in binary do
      bin == bin |> encode(:blob) |> decode(:blob)
    end
  end

  property "boolean" do
    for_all b in bool do
      b == b |> encode(:boolean) |> has_size(1) |> decode(:boolean)
    end
  end

  property "counter" do
    for_all n in int do
      n == n |> encode(:counter) |> has_size(8) |> decode(:counter)
    end
  end

  test "date" do
    date = DateTime.utc_now |> DateTime.to_date
    assert date == date |> encode(:date) |> has_size(4) |> decode(:date)
  end

  test "decimal" do
    xs = [
      {111222333444555666777888999000, 30},
      {-100200300400500600700800900, 89},
      {9374756681239761865712657819245, 98},
    ]
    for x <- xs do
      assert x == x |> encode(:decimal) |> drop_size |> decode(:decimal)
    end
  end

  property "decimal" do
    for_all x in {pos_integer, int} do
      x == x |> encode(:decimal) |> drop_size |> decode(:decimal)
    end
  end

  test "double" do
    xs = [
      1.2345,
      0.987654321,
      -23.591,
    ]
    for x <- xs do
      assert x == x |> encode(:double) |> has_size(8) |> decode(:double)
    end
  end

  test "float" do
    xs = [
      1.235,
      0.981,
      -23.590,
    ]
    for x <- xs do
      assert trunc(x * 1000) == x |> encode(:float) |> has_size(4) |> decode(:float) |> Kernel.*(1000) |> trunc
    end
  end

  test "inet" do
    nets = [
      {{127, 0, 0, 1}, 8123},
      {{192, 168, 100, 102}, 80},
    ]
    for net <- nets do
      assert net == net |> encode(:inet) |> has_size(9) |> decode(:inet)
    end
  end

  property "int" do
    for_all n in int do
      n == n |> encode(:int) |> has_size(4) |> decode(:int)
    end
  end

  test "list" do
    lists = [
      {:int, [10, 20, 30]},
      {:text, ["name", "example", "sample"]},
    ]
    for {type, list} <- lists do
      assert list == list |> encode({:list, type}) |> drop_size |> decode({:list, type})
    end
  end

  test "map" do
    maps = [
      {{:text, :int}, %{"a" => 10, "b" => 20, "c" => 30}},
      {{:text, :text}, %{"aaa" => "name", "bbb" => "example", "ccc" => "sample"}},
      {{:int, :double}, %{1 => 11.1, 10 => 22.2, 100 => 33.3}},
    ]
    for {type, map} <- maps do
      assert map == map |> encode({:map, type}) |> drop_size |> decode({:map, type})
    end
  end

  test "tuple" do
    types = [:int, :double, :text, :int, :float]
    tuple = {123,  23.983,  "Test", 91,  1.0}
    assert tuple == tuple |> encode({:tuple, types}) |> drop_size |> decode({:tuple, types})
  end

  test "varint" do
    xs = [
      9988776655443322110987654321,
      -19477209892471957969713409154091853,
      89769087908775467436532432,
      1000000000000000000000000000,
    ]
    for x <- xs do
      assert x == x |> encode(:varint) |> drop_size |> decode(:varint)
    end
  end

  defp drop_size(<<_::integer-32, rest::bytes>>), do: rest

  defp has_size(buffer, size) do
    <<n::integer-32, value::bytes>> = buffer
    if n == size do
      value
    else
      buffer
    end
  end
end
