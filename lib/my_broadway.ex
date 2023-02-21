defmodule MyBroadway do
  use Broadway

  require Logger

  @chunk_size 10

  @doc """
  Uses rate_limiting options to be able to see log messages in the console
  """
  def start_link(_opts) do
    options = [
      name: __MODULE__,
      producer: [
        module: {DBDummyProducer, []},
        transformer: {__MODULE__, :transform, []},
        rate_limiting: [
          interval: 10_000,
          allowed_messages: 10
        ],
        concurrency: 1
      ],
      processors: [
        default: [
          max_demand: @chunk_size,
          concurrency: 1
        ]
      ],
      batchers: [default: []]
    ]

    Broadway.start_link(__MODULE__, options)
  end

  def transform(event, _opts) do
    Logger.debug("[MyBroadway] transforming event: #{inspect(event)}")

    %Broadway.Message{
      data: event,
      acknowledger: {__MODULE__, :trackings, :ack_data}
    }
    |> Broadway.Message.put_batcher(:default)
  end

  def ack(:trackings, successful, failed) do
    Logger.debug(
      "[MyBroadway] ack successful: #{Enum.count(successful)}, failed: #{Enum.count(failed)}"
    )

    :ok
  end

  def handle_message(_processor, message, _context) do
    Logger.debug(fn -> "[MyBroadway] Incoming Message: #{inspect(message)}" end)

    message
  end

  def handle_batch(_batcher, messages, _batch_info, _context) do
    Logger.debug("[MyBroadway] in default batcher")
    Logger.debug(fn -> "[MyBroadway] size: #{length(messages)}" end)

    messages
  end
end
