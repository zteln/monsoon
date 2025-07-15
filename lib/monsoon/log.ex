defmodule Monsoon.Log do
  @moduledoc """
  commit_block:
  | (type::16-bit | root_loc::32-bit | leaf_links_loc::32-bit | metadata_loc::32-bit) |

  node_block:
  | (type::16-bit | id::64-bit | node-size::32-bit) | node |

  leaf_links_block:
  | (type::16-bit | size::32-bit) | leaf_links |

  metadata_block:
  | (type::16-bit | size::32-bit) | metadata |
  """

  alias Monsoon.BTree

  @commit_block_size 32
  @node_block_size 1024
  @leaf_links_block_size 512
  @metadata_block_size 512

  @commit_header_size byte_size(<<0::integer-16, 0::integer-32, 0::integer-32, 0::integer-32>>)
  @node_header_size byte_size(<<0::integer-16, 0::unsigned-integer-64, 0::integer-32>>)
  @leaf_links_header_size byte_size(<<0::integer-16, 0::integer-32>>)

  @commit_key 0xFAFA
  @node_key 0xFBFB
  @leaf_links_key 0xFCFC
  @metadata_key 0xFDFD

  defstruct [
    :pid,
    :file_path
  ]

  @type t :: %__MODULE__{
          pid: pid(),
          file_path: String.t()
        }

  @spec new(dir :: String.t()) :: Agent.on_start()
  def new(file_path) do
    with {:ok, pid} <-
           Agent.start_link(fn ->
             set_lock(file_path)
             init(file_path)
           end) do
      {:ok,
       %__MODULE__{
         pid: pid,
         file_path: file_path
       }}
    end
  end

  def move(from, to) do
    :ok = :file.rename(to.file_path, from.file_path)
    :ok = Agent.stop(from.pid)

    :ok =
      Agent.update(to.pid, fn state ->
        del_lock(to.file_path)
        set_lock(from.file_path)
        state
      end)

    %{to | file_path: from.file_path}
  end

  defp set_lock(file_path) do
    :global.set_lock({file_path, self()}, [node()], 0) ||
      raise "file resource `#{file_path}` already in use."
  end

  defp del_lock(file_path) do
    true = :global.del_lock({file_path, self()}, [node()])
  end

  defp init(file_path) do
    with {:ok, file} <- :file.open(file_path, [:binary, :read, :raw, :append]),
         {:ok, position} <- :file.position(file, :eof) do
      %{
        file: file,
        position: position,
        id_cache: %{},
        write_queue: :queue.new(),
        pre_flush_position: 0
      }
    end
  end

  @spec get_commit(log :: t()) :: {:ok, nil | non_neg_integer()} | {:error, term()}
  def get_commit(log) do
    Agent.get(log.pid, fn state ->
      read_latest_commit_block(state.file, state.position)
    end)
  end

  defp read_latest_commit_block(file, position) do
    position = max(position - @commit_block_size, 0)

    case :file.pread(file, position, @commit_header_size) do
      :eof ->
        {:ok, nil}

      {:ok,
       <<
         @commit_key::integer-16,
         root_loc::integer-32,
         leaf_links_loc::integer-32,
         metadata_loc::integer-32
       >>} ->
        {:ok, {root_loc, leaf_links_loc, metadata_loc}}

      {:ok, _} when position == 0 ->
        {:ok, nil}

      {:ok, _} ->
        read_latest_commit_block(file, position)

      {:error, _reason} = error ->
        error
    end
  end

  @spec put_node(log :: t(), node :: BTree.t()) :: {:ok, location :: non_neg_integer()}
  def put_node(log, %BTree.Leaf{} = node) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_node_block(node)
      write_queue = :queue.in({state.position, block}, state.write_queue)
      position = state.position + byte_size(block)
      id_cache = Map.put(state.id_cache, Map.get(node, :id), state.position)

      {{:ok, state.position},
       %{state | position: position, id_cache: id_cache, write_queue: write_queue}}
    end)
  end

  def put_node(log, %BTree.Interior{} = node) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_node_block(node)
      write_queue = :queue.in({state.position, block}, state.write_queue)
      position = state.position + byte_size(block)
      {{:ok, state.position}, %{state | position: position, write_queue: write_queue}}
    end)
  end

  def get_node_by_id(log, id) do
    Agent.get(log.pid, fn state ->
      block =
        case Map.get(state.id_cache, id) do
          nil ->
            # traverse
            {:ok, block} = read_node_from_id(state.file, state.position, id)
            block

          loc ->
            {:ok, block} = read_node_from_loc(state.file, loc)
            block
        end

      {:ok, decode_node_block(block)}
    end)
  end

  defp read_node_from_id(file, position, id) do
    position = max(position - @node_block_size, 0)

    case :file.pread(file, position, @node_header_size) do
      :eof ->
        {:ok, nil}

      {:ok, <<@node_key::integer-16, ^id::binary-size(8), size::integer-32>>} ->
        block_size =
          no_of_blocks(@node_header_size + size, @node_block_size) * @node_block_size

        :file.pread(
          file,
          position,
          block_size
        )

      {:ok, _} when position == 0 ->
        {:ok, nil}

      {:ok, _} ->
        read_node_from_id(file, position, id)

      {:error, _reason} = e ->
        e
    end
  end

  @spec get_node(log :: t(), loc :: non_neg_integer()) :: {:ok, BTree.t()}
  def get_node(log, loc) do
    Agent.get(log.pid, fn state ->
      {:ok, block} = read_node_from_loc(state.file, loc)
      {:ok, decode_node_block(block)}
    end)
  end

  defp read_node_from_loc(file, loc) do
    {:ok, <<@node_key::integer-16, _id::binary-size(8), size::integer-32>>} =
      :file.pread(file, loc, @node_header_size)

    block_size = no_of_blocks(@node_header_size + size, @node_block_size) * @node_block_size

    :file.pread(file, loc, block_size)
  end

  @spec commit(log :: t(), root_loc :: non_neg_integer()) :: :ok
  def commit(log, {root_loc, leaf_links_loc, metadata_loc}) do
    Agent.update(log.pid, fn state ->
      {:ok, position} =
        write_commit(state.file, state.position, {root_loc, leaf_links_loc, metadata_loc})

      %{state | position: position, pre_flush_position: position}
    end)
  end

  defp write_commit(file, position, {root_loc, leaf_links_loc, metadata_loc}) do
    block =
      <<
        @commit_key::integer-16,
        root_loc::integer-32,
        leaf_links_loc::integer-32,
        metadata_loc::integer-32,
        0::size(@commit_block_size - @commit_header_size)-unit(8)
      >>

    with :ok <- :file.pwrite(file, position, block),
         :ok <- :file.datasync(file) do
      {:ok, position + @commit_block_size}
    end
  end

  @spec put_leaf_links(log :: t(), dll :: map()) :: :ok
  def put_leaf_links(log, leaf_links) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_leaf_links_block(leaf_links)
      write_queue = :queue.in({state.position, block}, state.write_queue)
      position = state.position + byte_size(block)
      {{:ok, state.position}, %{state | position: position, write_queue: write_queue}}
    end)
  end

  @spec get_leaf_links(log :: t(), leaf_links_loc :: non_neg_integer()) :: {:ok, map()}
  def get_leaf_links(log, leaf_links_loc) do
    Agent.get(log.pid, fn state ->
      {:ok, block} = read_leaf_links_block(state.file, leaf_links_loc)
      {:ok, decode_leaf_links_block(block)}
    end)
  end

  defp read_leaf_links_block(file, loc) do
    {:ok, <<@leaf_links_key::integer-16, size::integer-32>>} =
      :file.pread(file, loc, @leaf_links_header_size)

    block_size =
      no_of_blocks(@leaf_links_header_size + size, @leaf_links_block_size) *
        @leaf_links_block_size

    :file.pread(file, loc, block_size)
  end

  def flush(log) do
    Agent.update(log.pid, fn state ->
      {:ok, position, write_queue} =
        flush_queue(state.file, state.pre_flush_position, state.write_queue)

      %{state | position: position, write_queue: write_queue, pre_flush_position: position}
    end)
  end

  defp flush_queue(file, position, queue) do
    with {:ok, new_position, queue, all_blocks} <- collect_blocks(position, queue, <<>>),
         :ok <- :file.pwrite(file, position, all_blocks) do
      {:ok, new_position, queue}
    end
  end

  defp collect_blocks(position, queue, acc) do
    case :queue.out(queue) do
      {:empty, queue} ->
        {:ok, position, queue, acc}

      {{:value, {^position, block}}, queue} ->
        collect_blocks(position + byte_size(block), queue, acc <> block)

      _ ->
        {:error, :wrong_position}
    end
  end

  defp encode_node_block(node) do
    id = if Map.has_key?(node, :id), do: Map.get(node, :id), else: <<0::unsigned-integer-64>>
    enc = :erlang.term_to_binary(node)
    node_size = byte_size(enc)
    block_size = byte_size(<<0::integer-16, 0::integer-64, 0::integer-32, enc::binary>>)
    no_of_blocks = no_of_blocks(block_size, @node_block_size)
    content = <<@node_key::integer-16, id::binary, node_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @node_block_size - block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_node_block(block) do
    <<@node_key::integer-16, _id::binary-size(8), _size::integer-32, bin::binary>> = block
    :erlang.binary_to_term(bin)
  end

  defp encode_leaf_links_block(ptrs) do
    enc = :erlang.term_to_binary(ptrs)
    ptrs_size = byte_size(enc)
    ptrs_block_size = byte_size(<<0::integer-16, 0::integer-32, enc::binary>>)
    no_of_blocks = no_of_blocks(ptrs_block_size, @leaf_links_block_size)
    content = <<@leaf_links_key::integer-16, ptrs_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @leaf_links_block_size - ptrs_block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_leaf_links_block(block) do
    <<@leaf_links_key::integer-16, _size::integer-32, enc::binary>> = block
    :erlang.binary_to_term(enc)
  end

  defp no_of_blocks(size, unit) do
    div(size + unit - 1, unit)
  end
end
