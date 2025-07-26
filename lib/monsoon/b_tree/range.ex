defmodule Monsoon.BTree.Range do
  defstruct [
    :min,
    :max,
    size: 0,
    list: []
  ]

  def has_overflow(range, max) do
    range.size >= max - 1
  end

  def has_underflow(range, max) do
    range.size < div(max, 2)
  end

  def add(%{size: 0} = range, _idx, el) do
    %{range | min: el, max: el, size: 1, list: [el]}
  end

  def add(range, idx, el) do
    list = List.insert_at(range.list, idx, el)

    %{
      range
      | list: list,
        size: range.size + 1,
        min: if(el < range.min, do: el, else: range.min),
        max: if(el > range.max, do: el, else: range.max)
    }

    # cond do
    #   el < range.min ->
    #     %{range | min: el, list: [el | range.list], size: range.size + 1}
    #
    #   el > range.max ->
    #     %{range | max: el, list: range.list ++ [el], size: range.size + 1}
    #
    #   true ->
    #     %{range | list: List.insert_at(range.list, idx, el), size: range.size + 1}
    # end
  end

  def replace(range, 0, el) do
    [_ | list] = range.list

    if range.size == 1 do
      %{range | min: el, max: el, list: [el | list]}
    else
      %{range | min: el, list: [el | list]}
    end
  end

  def replace(range, idx, el) do
    range =
      if idx == range.size - 1 do
        %{range | max: el}
      else
        range
      end

    %{range | list: List.replace_at(range.list, idx, el)}
  end

  def delete(range, 0) do
    if range.size == 1 do
      %__MODULE__{}
    else
      [_ | [min | _] = list] = range.list
      %{range | min: min, size: range.size - 1, list: list}
    end
  end

  def delete(range, idx) do
    range = %{range | list: List.delete_at(range.list, idx)}

    if idx == range.size - 1 do
      %{range | max: List.last(range.list), size: range.size - 1}
    else
      %{range | size: range.size - 1}
    end
  end

  def find(range, el, el_trans \\ & &1) do
    cond do
      range.size == 0 ->
        nil

      el_trans.(range.min) == el ->
        range.min

      el_trans.(range.max) == el ->
        range.max

      true ->
        Enum.find(range.list, fn x -> el_trans.(x) == el end)
    end
  end

  def find_index(range, el, el_trans \\ & &1) do
    cond do
      range.size == 0 ->
        {:next, 0}

      el < el_trans.(range.min) ->
        {:next, 0}

      el > el_trans.(range.max) ->
        {:next, range.size}

      true ->
        search_for_index(range.list, el, 0, el_trans)
    end
  end

  defp search_for_index([], _el, idx, _el_trans), do: {:next, idx}

  defp search_for_index([curr | list], el, idx, el_trans) do
    cond do
      el_trans.(curr) == el ->
        {:exact, idx}

      el_trans.(curr) > el ->
        {:next, idx}

      true ->
        search_for_index(list, el, idx + 1, el_trans)
    end
  end

  def peek_at(%{min: min}, 0), do: min
  def peek_at(%{min: min}, -1), do: min

  def peek_at(range, idx) do
    Enum.at(range.list, idx)
  end

  def pop_at(%{size: 1} = range, _idx) do
    [pop] = range.list
    {%__MODULE__{}, pop}
  end

  def pop_at(range, 0) do
    [pop | [min | _] = list] = range.list
    {%{range | list: list, min: min, size: range.size - 1}, pop}
  end

  def pop_at(range, -1) do
    {pop, list} = List.pop_at(range.list, -1)
    {%{range | list: list, max: List.last(list), size: range.size - 1}, pop}
  end

  def pop_at(range, idx) do
    {pop, list} = List.pop_at(range.list, idx)
    {%{range | list: list, size: range.size - 1}, pop}
  end

  def split(range, at, inclusive? \\ false) do
    {llist, [mid | rlist_excl] = rlist} = Enum.split(range.list, at)
    rlist = if inclusive?, do: rlist, else: rlist_excl
    rsize = if inclusive?, do: range.size - at, else: range.size - at - 1

    left =
      %__MODULE__{
        min: List.first(llist),
        max: List.last(llist),
        list: llist,
        size: at
      }

    right =
      %__MODULE__{
        min: List.first(rlist),
        max: List.last(rlist),
        list: rlist,
        size: rsize
      }

    {left, mid, right}
  end

  def merge(lrange, rrange, mid \\ nil) do
    merged_list = lrange.list ++ List.wrap(mid) ++ rrange.list
    size = if mid, do: lrange.size + 1 + rrange.size, else: lrange.size + rrange.size

    %__MODULE__{
      list: merged_list,
      size: size,
      min: lrange.min,
      max: rrange.max
    }
  end

  def take(range, nil, nil, _trans), do: {:cont, range.list}

  # TODO: consider when either lower or upper is nil 
  # (nil, upper) -> no lower bound
  # (lower, nil) -> no upper bound
  def take(range, lower, upper, trans) do
    take_from_list(range.list, lower, upper, trans, [])
  end

  defp take_from_list([], _lower, _upper, _trans, acc), do: {:cont, Enum.reverse(acc)}

  defp take_from_list([hd | tl], lower, upper, trans, acc) do
    k = trans.(hd)

    cond do
      k < lower ->
        take_from_list(tl, lower, upper, trans, acc)

      lower <= k and k <= upper ->
        take_from_list(tl, lower, upper, trans, [hd | acc])

      k > upper ->
        {:halt, Enum.reverse(acc)}
    end
  end
end
