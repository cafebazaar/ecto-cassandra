defmodule Cassandra do
  alias Cassandra.{Cluster, Session}

  defmacro __using__(opts \\ []) do
    quote do
      use Supervisor

      @cluster __MODULE__.Cluster
      @session __MODULE__.Session

      def start_link(options) do
        Supervisor.start_link(__MODULE__, options)
      end

      def init(options) do
        config = case Keyword.fetch(unquote(opts), :otp_app) do
          {:ok, app} ->
            Application.get_env(app, __MODULE__, [])
          :error ->
            []
        end

        options = Keyword.merge(config, options)

        {contact_points, options} = Keyword.pop(options, :contact_points, ["127.0.0.1"])

        children = [
          worker(Cluster, [contact_points, [], [name: @cluster]]),
          worker(Session, [@cluster, options, [name: @session]])
        ]

        supervise(children, strategy: :rest_for_one)
      end

      def send(request) do
        Session.send(@session, request)
      end
    end
  end
end
