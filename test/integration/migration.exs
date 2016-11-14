defmodule EctoCassandra.Integration.Migration do
  use Ecto.Migration

  def change do
    create table(:users, comment: "users table", primary_key: false) do
      add :id, :id, partition_key: true
      add :name, :text

      timestamps
    end

    create table(:posts, primary_key: false) do
      add :title, :text, partition_key: true
      add :counter, :integer
      add :text, :binary
      add :bid, :binary_id
      add :uuid, :uuid
      add :meta, :map
      add :links, {:map, :string}
      add :public, :boolean
      add :cost, :decimal
      add :visits, :integer
      add :intensity, :float
      add :author_id, :integer
      add :posted, :date
      add :ip, :inet
      timestamps
    end
  end
end
