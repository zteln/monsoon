defmodule Monsoon.Log do
  @moduledoc """
  root_ptr_block:
  | type::16-bit | root_position::32-bit |

  node_block:
  | type::16-bit | node-size::32-bit | node |

  metadata_block:
  | type::16-bit | metadata-size::32-bit | metadata |
  """

  alias Monsoon.BTree

  @root_ptr_block_size byte_size(<<0::integer-16, 0::integer-32>>)
  @node_block_size 1024
  @node_block_header_size 6
  @root_ptr_block_key 0xFAFA
  @node_block_key 0xFBFB
  # @metadata_block_key 0xFCFC

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

  def stop(log) do
    Agent.stop(log.pid)
  end

  def rename(log, prev_log) do
    :ok = :file.rename(log.file_path, prev_log.file_path)

    Agent.update(prev_log.pid, fn state ->
      del_lock(prev_log.file_path)
      state
    end)

    Agent.update(log.pid, fn state ->
      del_lock(log.file_path)
      set_lock(prev_log.file_path)
      state
    end)

    %{log | file_path: prev_log.file_path}
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
      %{file: file, position: position}
    end
  end

  @spec latest_root_location(log :: t()) :: {:ok, nil | non_neg_integer()} | {:error, term()}
  def latest_root_location(log) do
    Agent.get(log.pid, fn state ->
      read_latest_root_ptr_block(state.file, state.position)
    end)
  end

  defp read_latest_root_ptr_block(file, position) do
    position = max(position - @root_ptr_block_size, 0)

    case :file.pread(file, position, @root_ptr_block_size) do
      :eof ->
        {:ok, nil}

      {:ok, <<@root_ptr_block_key::integer-16, root_loc::integer-32>>} ->
        {:ok, root_loc}

      {:ok, _} when position == 0 ->
        {:ok, nil}

      {:ok, _} ->
        read_latest_root_ptr_block(file, position)

      {:error, _reason} = error ->
        error
    end
  end

  @spec put_node(log :: t(), node :: BTree.t()) :: {:ok, location :: non_neg_integer()}
  def put_node(log, node) do
    Agent.get_and_update(log.pid, fn state ->
      {:ok, node_loc, position} = write_node(state.file, state.position, node)
      {{:ok, node_loc}, %{state | position: position}}
    end)
  end

  defp write_node(file, loc, node) do
    block = encode_node_block(node)

    with :ok <- :file.pwrite(file, loc, block) do
      # :ok <- :file.datasync(file) do
      write_loc = loc
      loc = loc + byte_size(block)
      {:ok, write_loc, loc}
    end
  end

  @spec get_node(log :: t(), loc :: non_neg_integer()) :: {:ok, BTree.t()}
  def get_node(log, loc) do
    Agent.get(log.pid, fn state ->
      read_node_from_loc(state.file, loc)
    end)
  end

  defp read_node_from_loc(file, loc) do
    {:ok, <<@node_block_key::integer-16, size::integer-32>>} =
      :file.pread(file, loc, @node_block_header_size)

    {:ok, block} =
      :file.pread(file, loc, no_of_blocks(@node_block_header_size + size) * @node_block_size)

    {:ok, decode_node_block(block)}
  end

  @spec commit(log :: t(), root_loc :: non_neg_integer()) :: :ok
  def commit(log, root_loc) do
    Agent.update(log.pid, fn state ->
      {:ok, position} = write_root_ptr(state.file, state.position, root_loc)
      %{state | position: position}
    end)
  end

  defp write_root_ptr(file, position, root_loc) do
    block = <<@root_ptr_block_key::integer-16, root_loc::integer-32>>

    with :ok <- :file.pwrite(file, position, block),
         :ok <- :file.datasync(file) do
      {:ok, position + @root_ptr_block_size}
    end
  end

  defp encode_node_block(node) do
    enc = :erlang.term_to_binary(node)
    node_size = byte_size(enc)
    block_size = byte_size(<<0::integer-16, 0::integer-32, enc::binary>>)
    no_of_blocks = no_of_blocks(block_size)
    content = <<@node_block_key::integer-16, node_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @node_block_size - block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_node_block(block) do
    <<@node_block_key::integer-16, _size::integer-32, bin::binary>> = block
    :erlang.binary_to_term(bin)
  end

  defp no_of_blocks(size) do
    div(size + @node_block_size - 1, @node_block_size)
  end
end
