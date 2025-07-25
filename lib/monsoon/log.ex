defmodule Monsoon.Log do
  @moduledoc """
  Handles writing and reading to and from the log file.
  There are four different blocks that are written to the log: a commit block, a node block, a leaf links block, a metadata block.
  Each block has a unit size of 1024 bytes.

  ## Block specifications:

  Commit block:
  Contains information about the latest commit. The latest commit is the first commit block when reading backwards for end-of-file position.
      
      <<
        0xFAFA::integer-16, 
        root_loc::integer-32,
        root_size::integer-32,
        leaf_links_loc::integer-32,
        leaf_links_size::integer-32,
        metadata_loc::integer-32,
        metadata_size::integer-32,
        _::binary
      >>

  Node block:
  Contains a node, either leaf or interior. Specifies `id` and `size`.

      <<
        0xFBFB::integer-16,
        id::integer-64,
        size::integer-32,
        node::binary
      >>

  Leaf links block:
  Contains the leaf links (sibling links between leafs). Specifies the size of the leaf links.

      <<
        0xFCFC::integer-16,
        size::integer-32,
        leaf_links::binary
      >>

  Metadata block:
  Contains the metadata data. Specifies the size of the encoded metadata.

      <<
        0xFDFD::integer-16,
        size::integer-32,
        metadata::binary
      >>
  """

  alias Monsoon.BTree

  @block_size 1024

  @commit_header_size byte_size(<<
                        0::integer-16,
                        0::integer-32,
                        0::integer-32,
                        0::integer-32,
                        0::integer-32,
                        0::integer-32,
                        0::integer-32
                      >>)
  @node_header_size byte_size(<<
                      0::integer-16,
                      0::unsigned-integer-64,
                      0::integer-32
                    >>)

  @leaf_links_header_size byte_size(<<
                            0::integer-16,
                            0::integer-32
                          >>)
  @metadata_header_size byte_size(<<
                          0::integer-16,
                          0::integer-32
                        >>)

  @commit_key 0xFAFA
  @node_key 0xFBFB
  @leaf_links_key 0xFCFC
  @metadata_key 0xFDFD

  defstruct [
    :pid,
    :file_path
  ]

  @type block_pointer :: {location :: non_neg_integer(), size :: non_neg_integer()}

  @type t :: %__MODULE__{
          pid: pid(),
          file_path: String.t()
        }

  @type state :: %{
          file: :file.io_device(),
          position: non_neg_integer(),
          id_cache: map(),
          write_queue: :queue.queue(),
          pre_flush_position: non_neg_integer()
        }

  @spec new(file_path :: String.t()) :: Agent.on_start()
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

  defp init(file_path) do
    with {:ok, file} <- :file.open(file_path, [:binary, :read, :raw, :append]),
         {:ok, position} <- :file.position(file, :eof) do
      %{
        file: file,
        position: position,
        id_cache: %{},
        write_queue: :queue.new(),
        pre_flush_position: position
      }
    end
  end

  @spec move(from :: t(), to :: t()) :: t()
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

  @spec get_commit(log :: t()) :: {:ok, nil | non_neg_integer()} | {:error, term()}
  def get_commit(log) do
    Agent.get(log.pid, fn state ->
      with {:ok, header, _loc} <-
             find_block_header(
               state.file,
               state.position,
               @block_size,
               @commit_header_size,
               <<@commit_key::integer-16>>
             ) do
        decode_commit_block(header)
      end
    end)
  end

  @spec commit(
          log :: t(),
          root_loc :: non_neg_integer(),
          leaf_links_loc :: non_neg_integer(),
          metadata_loc :: non_neg_integer()
        ) :: :ok
  def commit(log, root_bp, leaf_links_bp, metadata_bp) do
    Agent.update(log.pid, fn state ->
      block = encode_commit_block(root_bp, leaf_links_bp, metadata_bp)

      {_position, write_queue} = enqueue(state.write_queue, block, state.position)

      {:ok, position, write_queue} =
        flush_queue(state.file, state.pre_flush_position, write_queue)

      :ok = :file.datasync(state.file)

      %{state | position: position, pre_flush_position: position, write_queue: write_queue}
    end)
  end

  @spec put_node(log :: t(), node :: BTree.t()) :: BTree.child()
  def put_node(log, %BTree.Leaf{} = node) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_node_block(node)
      loc_and_size = {state.position, byte_size(block)}
      {position, write_queue} = enqueue(state.write_queue, block, state.position)
      id_cache = Map.put(state.id_cache, Map.get(node, :id), loc_and_size)

      {loc_and_size, %{state | position: position, id_cache: id_cache, write_queue: write_queue}}
    end)
  end

  def put_node(log, %BTree.Interior{} = node) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_node_block(node)
      {position, write_queue} = enqueue(state.write_queue, block, state.position)

      {{state.position, byte_size(block)},
       %{state | position: position, write_queue: write_queue}}
    end)
  end

  @spec get_node_by_id(log :: t(), id :: binary()) :: {:ok, BTree.t()} | {:error, term()}
  def get_node_by_id(log, id) do
    Agent.get(log.pid, fn state ->
      case Map.get(state.id_cache, id) do
        nil ->
          # traverse
          with {:ok, <<@node_key::integer-16, ^id::binary-size(8), size::integer-32>>} <-
                 find_block_header(
                   state.file,
                   state.position,
                   @block_size,
                   @node_header_size,
                   <<@node_key::integer-16, id::binary-size(8)>>
                 ),
               {:ok, block} <-
                 :file.pread(
                   state.file,
                   state.position,
                   no_of_blocks(@node_header_size + size, @block_size) * @block_size
                 ) do
            decode_node_block(block)
          end

        {loc, size} ->
          with {:ok, block} <- :file.pread(state.file, loc, size) do
            decode_node_block(block)
          end
      end
    end)
  end

  @spec get_node(log :: t(), BTree.child()) :: {:ok, BTree.t()} | {:error, term()}
  def get_node(log, {loc, size}) do
    Agent.get(log.pid, fn state ->
      with {:ok, block} <- :file.pread(state.file, loc, size) do
        decode_node_block(block)
      end
    end)
  end

  @spec put_leaf_links(log :: t(), leaf_links :: map()) :: block_pointer()
  def put_leaf_links(log, leaf_links) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_leaf_links_block(leaf_links)
      {position, write_queue} = enqueue(state.write_queue, block, state.position)

      {{state.position, byte_size(block)},
       %{state | position: position, write_queue: write_queue}}
    end)
  end

  @spec get_leaf_links(log :: t(), leaf_links_bp :: block_pointer()) ::
          {:ok, map()} | {:error, term()}
  def get_leaf_links(log, {loc, size}) do
    Agent.get(log.pid, fn state ->
      with {:ok, block} <- :file.pread(state.file, loc, size) do
        decode_leaf_links_block(block)
      end
    end)
  end

  @spec put_metadata(log :: t(), metadata :: keyword()) :: block_pointer()
  def put_metadata(log, metadata) do
    Agent.get_and_update(log.pid, fn state ->
      block = encode_metadata_block(metadata)
      {position, write_queue} = enqueue(state.write_queue, block, state.position)

      {{state.position, byte_size(block)},
       %{state | position: position, write_queue: write_queue}}
    end)
  end

  @spec get_metadata(log :: t(), metadata_bp :: block_pointer()) ::
          {:ok, keyword()} | {:error, term()}
  def get_metadata(log, {loc, size}) do
    Agent.get(log.pid, fn state ->
      with {:ok, block} <- :file.pread(state.file, loc, size) do
        decode_metadata_block(block)
      end
    end)
  end

  @spec flush(log :: t()) :: :ok
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

  defp enqueue(queue, block, position) do
    queue = :queue.in({position, block}, queue)
    {position + byte_size(block), queue}
  end

  defp encode_commit_block(
         {root_loc, root_size},
         {leaf_links_loc, leaf_links_size},
         {metadata_loc, metadata_size}
       ) do
    <<
      @commit_key::integer-16,
      root_loc::integer-32,
      root_size::integer-32,
      leaf_links_loc::integer-32,
      leaf_links_size::integer-32,
      metadata_loc::integer-32,
      metadata_size::integer-32,
      0::size(@block_size - @commit_header_size)-unit(8)
    >>
  end

  defp decode_commit_block(block) do
    case block do
      <<
        @commit_key::integer-16,
        root_loc::integer-32,
        root_size::integer-32,
        leaf_links_loc::integer-32,
        leaf_links_size::integer-32,
        metadata_loc::integer-32,
        metadata_size::integer-32
      >> ->
        {:ok, {root_loc, root_size}, {leaf_links_loc, leaf_links_size},
         {metadata_loc, metadata_size}}

      _ ->
        {:error, :unable_to_decode_commit}
    end
  end

  defp encode_node_block(node) do
    id = Map.get(node, :id, <<0::unsigned-integer-64>>)
    enc = :erlang.term_to_binary(node)
    node_size = byte_size(enc)
    block_size = byte_size(<<0::integer-16, 0::integer-64, 0::integer-32, enc::binary>>)
    no_of_blocks = no_of_blocks(block_size, @block_size)
    content = <<@node_key::integer-16, id::binary, node_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @block_size - block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_node_block(block) do
    case block do
      <<@node_key::integer-16, _id::binary-size(8), _size::integer-32, bin::binary>> ->
        {:ok, :erlang.binary_to_term(bin)}

      _ ->
        {:error, :unable_to_decode_node}
    end
  end

  defp encode_leaf_links_block(leaf_links) do
    enc = :erlang.term_to_binary(leaf_links)
    leaf_links_size = byte_size(enc)
    block_size = @leaf_links_header_size + leaf_links_size
    no_of_blocks = no_of_blocks(block_size, @block_size)
    content = <<@leaf_links_key::integer-16, leaf_links_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @block_size - block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_leaf_links_block(block) do
    case block do
      <<@leaf_links_key::integer-16, _size::integer-32, enc::binary>> ->
        {:ok, :erlang.binary_to_term(enc)}

      _ ->
        {:error, :unable_to_decode_leaf_links}
    end
  end

  defp encode_metadata_block(metadata) do
    enc = :erlang.term_to_binary(metadata)
    metadata_size = byte_size(enc)
    block_size = @metadata_header_size + metadata_size
    no_of_blocks = no_of_blocks(block_size, @block_size)
    content = <<@metadata_key::integer-16, metadata_size::integer-32, enc::binary>>
    padding_size = no_of_blocks * @block_size - block_size
    <<content::binary, 0::size(padding_size)-unit(8)>>
  end

  defp decode_metadata_block(block) do
    case block do
      <<@metadata_key::integer-16, _size::integer-32, enc::binary>> ->
        {:ok, :erlang.binary_to_term(enc)}

      _ ->
        {:error, :unable_to_decode_metadata}
    end
  end

  defp find_block_header(file, position, block_size, header_size, match) do
    position = max(position - block_size, 0)

    case :file.pread(file, position, header_size) do
      {:ok, ^match <> <<_rest::binary>> = header} ->
        {:ok, header, position}

      {:ok, _} when position == 0 ->
        {:error, :not_found}

      {:ok, _} ->
        find_block_header(file, position, block_size, header_size, match)

      :eof ->
        {:error, :eof}

      {:error, _reason} = e ->
        e
    end
  end

  defp no_of_blocks(size, unit) do
    div(size + unit - 1, unit)
  end
end
