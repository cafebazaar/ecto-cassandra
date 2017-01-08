defmodule EctoCassandra.Integration.User do
  use Ecto.Schema
  import Ecto.Changeset

  @timestamps_opts [usec: true]
  @primary_key {:id, :id, autogenerate: true}
  schema "users" do
    field :name
    field :hobbes, {:array, :string}
    has_many :posts, EctoCassandra.Integration.Post, foreign_key: :author_id
    timestamps type: :utc_datetime
  end
end

defmodule EctoCassandra.Integration.Post do
  use Ecto.Schema
  import Ecto.Changeset

  @timestamps_opts [usec: true]
  @foreign_key_type :id
  @primary_key {:title, :string, autogenerate: false}
  schema "posts" do
    field :counter, :integer
    field :text, :binary
    field :temp, :string, default: "temp", virtual: true
    field :public, :boolean, default: true
    field :cost, :decimal
    field :visits, :integer
    field :intensity, :float
    field :uuid, :binary_id
    field :timeuuid, :binary_id
    field :meta, :map
    field :links, {:map, :string}
    field :posted, :date
    field :ip, EctoCassandra.INet
    belongs_to :author, EctoCassandra.Integration.User
    timestamps()
  end

  def changeset(schema, params) do
    cast(schema, params, ~w(counter title text temp public cost visits
                          intensity bid uuid meta posted))
  end
end
