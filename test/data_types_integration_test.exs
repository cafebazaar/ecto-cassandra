defmodule DataTypesIntegrationTest do
  use ExUnit.Case

  alias Cassandra.Connection
  alias UUID

  setup_all do
    {:ok, connection} = Connection.start_link(keyspace: "elixir_cassandra_test")
    {:ok, _} = Connection.query connection, """
      CREATE TABLE data_types (
        f_ascii     ascii,
        f_bigint    bigint,
        f_blob      blob,
        f_boolean   boolean,
        f_date      date,
        f_decimal   decimal,
        f_double    double,
        f_float     float,
        f_inet      inet,
        f_int       int,
        f_smallint  smallint,
        f_text      text,
        f_time      time,
        f_timestamp timestamp,
        f_timeuuid  timeuuid,
        f_tinyint   tinyint,
        f_uuid      uuid,
        f_varchar   varchar,
        f_varint    varint,
        f_map1      map<text, text>,
        f_map2      map<int, boolean>,
        f_list1     list<text>,
        f_list2     list<int>,
        f_set       set<int>,
        PRIMARY KEY (f_timeuuid, f_timestamp, f_uuid)
      );
    """
    {:ok, %{connection: connection}}
  end

  setup %{connection: connection} do
    Connection.query(connection, "TRUNCATE data_types;")
    {:ok, %{connection: connection}}
  end

  test "DATA TYPES", %{connection: connection} do
    {:ok, prepared} = Connection.prepare connection, """
      INSERT INTO data_types (
        f_ascii,
        f_bigint,
        f_blob,
        f_boolean,
        f_date,
        f_decimal,
        f_double,
        f_float,
        f_inet,
        f_int,
        f_smallint,
        f_text,
        f_time,
        f_timestamp,
        f_timeuuid,
        f_tinyint,
        f_uuid,
        f_varchar,
        f_varint,
        f_map1,
        f_map2,
        f_list1,
        f_list2,
        f_set
      ) VALUES (
        :f_ascii,
        :f_bigint,
        :f_blob,
        :f_boolean,
        :f_date,
        :f_decimal,
        :f_double,
        :f_float,
        :f_inet,
        :f_int,
        :f_smallint,
        :f_text,
        :f_time,
        :f_timestamp,
        :f_timeuuid,
        :f_tinyint,
        :f_uuid,
        :f_varchar,
        :f_varint,
        :f_map1,
        :f_map2,
        :f_list1,
        :f_list2,
        :f_set
      );
    """
    data = %{
      f_ascii: "abcdefgh",
      f_bigint: 1000000000,
      f_blob: <<1,2,3,4,5,6,7,8,9,10,11,12>>,
      f_boolean: true,
      f_date: ~D[2016-12-18],
      f_decimal: {12345, 6},
      f_double: 1.985,
      f_float: -1.5,
      f_inet: {127, 0, 0, 1},
      f_int: 1000000,
      f_smallint: 100,
      f_text: "Hello World",
      f_time: 1476019457741087551,
      f_timestamp: 1476019507396,
      f_timeuuid: UUID.uuid1(),
      f_tinyint: 1,
      f_uuid: UUID.uuid4(),
      f_varchar: "Some یونی کد string",
      f_varint: 1122334455667788990099887766,
      f_map1: %{"foo" => "bar", "baz" => "biz"},
      f_map2: %{1 => true, 2 => false},
      f_list1: ["a", "bb", "ccc", "dddd"],
      f_list2: [10, 20, 30, 40],
      f_set: MapSet.new([10, 20, 10, 20, 30]),
    }
    assert {:ok, :done} = Connection.execute(connection, prepared, data)
    assert {:ok, [result]} = Connection.query(connection, "SELECT * FROM data_types LIMIT 1;")
    assert true =
      result
      |> Map.values
      |> Enum.zip(Map.values(data))
      |> Enum.map(fn {a, b} -> a == b end)
      |> Enum.all?
  end
end
