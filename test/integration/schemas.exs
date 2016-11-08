defmodule EctoCassandra.Integration.Schema do
  defmacro __using__(_) do
    quote do
      use Ecto.Schema

      # @primary_key {:id, :binary_id, autogenerate: true}
      @primary_key false
      @foreign_key_type :binary_id
      @timestamps_opts [usec: true]
    end
  end
end

defmodule EctoCassandra.Integration.Post do
  use EctoCassandra.Integration.Schema
  import Ecto.Changeset

  schema "posts" do
    field :title, :string, primary_key: true
    field :counter, :integer
    field :text, :binary
    field :temp, :string, default: "temp", virtual: true
    field :public, :boolean, default: true
    field :cost, :decimal
    field :visits, :integer
    field :intensity, :float
    field :bid, :binary_id
    field :uuid, :string
    field :meta, :map
    field :links, {:map, :string}
    field :posted, :date
    timestamps
  end

  def changeset(schema, params) do
    cast(schema, params, ~w(counter title text temp public cost visits
                          intensity bid uuid meta posted))
  end
end
