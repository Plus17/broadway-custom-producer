defmodule DBDummyProducer do
  @moduledoc """
  This module emulates database behavior by producing in-memory rows
  """
  use GenStage

  require Logger

  @impl GenStage
  def init(opts) do
    {:producer, opts}
  end

  def start_link(number) do
    GenStage.start_link(__MODULE__, number)
  end

  @impl true
  @spec handle_demand(integer, any) :: {:noreply, list, any}
  def handle_demand(demand, opts) do
    Logger.debug(fn ->
      "[DBDummyProducer] handling demand: #{demand}"
    end)

    # events = Users.list_users(limit: demand)
    events =
      Enum.map(1..demand, fn _number ->
        %{
          id: Enum.random(1..9_999_999_999)
        }
      end)

    {:noreply, events, opts}
  end
end
