defmodule CassandraTest do
  use ExUnit.Case
  doctest Cassandra

  alias Cassandra.Protocol
  alias CQL.{Options, Supported, Query, Prepare, Execute, QueryParams}

  setup do
    {:ok, %{socket: socket}} = Protocol.connect([])
    {:ok, %{socket: socket}}
  end

  test "OPTIONS", %{socket: socket} do
    assert :ok = Protocol.send_request(%Options{}, socket)
    assert %Supported{options: options} = Protocol.receive_responce(socket)
    assert ["CQL_VERSION", "COMPRESSION"] = Keyword.keys(options)
  end

  test "QUERY", %{socket: socket} do
    # q = %Query{query: """
    #       create keyspace elixir_cql_test
    #       with replication = {'class':'SimpleStrategy','replication_factor':1};
    #     """}
    # assert :ok = Protocol.send_request(q, socket)
    # IO.inspect Protocol.receive_responce(socket)

    # q = %Query{query: "USE elixir_cql_test;"}
    # assert :ok = Protocol.send_request(q, socket)
    # IO.inspect Protocol.receive_responce(socket)

    # q = %Query{query: """
    #     create table emp (
    #       empid int primary key,
    #       emp_first varchar,
    #       emp_last varchar,
    #       emp_dept varchar
    #     );
    #     """}
    # assert :ok = Protocol.send_request(q, socket)
    # IO.inspect Protocol.receive_responce(socket)

    # q = %Query{query: """
    #     insert into emp (empid, emp_first, emp_last, emp_dept)
    #       values (1,'fred','smith','eng');
    #     """}
    # assert :ok = Protocol.send_request(q, socket)
    # IO.inspect Protocol.receive_responce(socket)

    # q = %Query{query: "select * from emp;"}
    # assert :ok = Protocol.send_request(q, socket)
    # IO.inspect Protocol.receive_responce(socket)
  end

  test "PREPARE", %{socket: socket} do
    q = %Query{query: "USE elixir_cql_test;"}
    assert :ok = Protocol.send_request(q, socket)
    IO.inspect Protocol.receive_responce(socket)

    q = %Prepare{query: "insert into emp (empid, emp_first, emp_last, emp_dept)
 values (?, ?, ?, ?);"}
    assert :ok = Protocol.send_request(q, socket)
    %{id: id} = IO.inspect(Protocol.receive_responce(socket))

    q = %Execute{id: id, params: %QueryParams{
      values: [10, "Akbar", "Sadeghi", "1000"],
      consistency: :ONE,
    }}
    assert :ok = Protocol.send_request(q, socket)
    IO.inspect(Protocol.receive_responce(socket))

    q = %Query{query: "select * from emp;"}
    assert :ok = Protocol.send_request(q, socket)
    IO.inspect Protocol.receive_responce(socket)
  end
end
