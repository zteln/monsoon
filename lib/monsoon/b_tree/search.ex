defmodule Monsoon.BTree.Search do
  alias Monsoon.Log

  @spec search_key(log :: GenServer.server(), node :: Monsoon.BTree.t(), key :: term) ::
          {:ok, term()} | {:error, nil | term()}
  def search_key(_log, %{is_leaf: true} = node, key) do
    case Enum.find(node.pairs, fn {k, _v} -> k == key end) do
      nil ->
        {:error, nil}

      {_k, v} ->
        {:ok, v}
    end
  end

  def search_key(log, node, key) do
    node.pairs
    |> Enum.with_index()
    |> Enum.reduce_while(0, fn
      {{k, v}, _idx}, _acc when k == key ->
        {:halt, {:ok, v}}

      {{k, _v}, idx}, _acc when k > key ->
        child_pos = Enum.at(node.children, idx)

        res =
          with {:ok, child} <- Log.get_node(log, child_pos) do
            search_key(log, child, key)
          end

        {:halt, res}

      {{k, _v}, idx}, _acc when k < key ->
        {:cont, idx + 1}
    end)
    |> case do
      idx when is_number(idx) ->
        child_pos = Enum.at(node.children, idx)

        with {:ok, child} <- Log.get_node(log, child_pos) do
          search_key(log, child, key)
        end

      res ->
        res
    end
  end

  # @spec select_keys(
  #         log :: GenServer.server(),
  #         node :: BTree.t(),
  #         lower :: term(),
  #         upper :: term(),
  #         acc :: list()
  #       ) :: list()
  # def select_keys(_log, _node, _lower, _upper, {:error, _} = acc), do: acc
  #
  # def select_keys(_log, %{is_leaf: true} = node, lower, upper, acc) do
  #   Enum.reduce(node.pairs, acc, fn
  #     {k, v}, acc ->
  #       if k >= lower and k <= upper do
  #         [{k, v} | acc]
  #       else
  #         acc
  #       end
  #   end)
  # end
  #
  # def select_keys(log, node, lower, upper, acc) do
  #   pairs_len = length(node.pairs)
  #
  #   node.pairs
  #   |> Enum.with_index()
  #   |> Enum.reduce(acc, fn
  #     _, {:error, _} = acc ->
  #       acc
  #
  #     {{k, _v}, _idx}, acc when k < lower ->
  #       acc
  #
  #     {{k, v}, idx}, acc when k > upper ->
  #       with {:ok, child} <- get_child(log, node, idx) do
  #         select_keys(log, child, lower, upper, [{k, v} | acc])
  #       end
  #
  #     {{k, v}, idx}, acc when idx == pairs_len - 1 ->
  #       with {:ok, child} <- get_child(log, node, idx),
  #            {:ok, last_child} <- get_child(log, node, idx + 1) do
  #         acc = select_keys(log, child, lower, upper, [{k, v} | acc])
  #         select_keys(log, last_child, lower, upper, acc)
  #       end
  #
  #     {{k, v}, idx}, acc ->
  #       with {:ok, child} <- get_child(log, node, idx) do
  #         select_keys(log, child, lower, upper, [{k, v} | acc])
  #       end
  #   end)
  # end
  #
  # defp get_child(log, node, idx) do
  #   child_pos = Enum.at(node.children, idx)
  #   Log.get_node(log, child_pos)
  # end
end
