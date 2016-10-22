defmodule Cassandra do
  alias Cassandra.{Cluster, Session}

  defmacro __using__(opts \\ []) do
    quote do
      def start_link(options) do
        config = case Keyword.fetch(unquote(opts), :otp_app) do
          {:ok, app} ->
            Application.get_env(app, __MODULE__, [])
          :error ->
            []
        end

        options = Keyword.merge(config, options)

        {contact_points, options} = Keyword.pop(options, :contact_points, ["127.0.0.1"])

        with {:ok, cluster} <- Cluster.start_link(contact_points, name: __MODULE__.Cluster) do
          options = Keyword.put(options, :name, __MODULE__.Session)
          Cluster.connect_link(cluster, options)
        end
      end

      def send(request) do
        Session.send(__MODULE__.Session, request)
      end
    end
  end
end
