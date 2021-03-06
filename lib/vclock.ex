defmodule Crdtex.Vclock do
  @moduledoc """
  A simple Erlang implementation of vector clocks as inspired by Lamport logical clocks.
  
  references : 
  - Leslie Lamport (1978). *Time, clocks, and the ordering of events
    in a distributed system*. Communications of the ACM 21 (7): 558-565.
    [link](http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf)
  - Friedemann Mattern (1988). *Virtual Time and Global States of
    Distributed Systems*. Workshop on Parallel and Distributed Algorithms: pp. 215-226
    [link](http://homes.cs.washington.edu/~arvind/cs425/doc/mattern89virtual.pdf)

  """
  @type t :: [vc_entry]
  @type binary_vclock :: binary
  @typedoc "The timestamp is present but not used, in case a client wishes to inspect it"
  @type vc_entry :: {vclock_node, counter}
  @typedoc "Nodes can have any term as a name, but they must differ from each other."
  @type   vclock_node :: term
  @type   counter :: integer

  @spec fresh() :: t
  def fresh, do: []

  @doc "Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!"
  @spec descends(va :: t|[], vb :: t|[]) :: boolean
  def descends(_, []), do: true # all vclocks descend from the empty vclock
  def descends(va, [{node_b, ctr_b} |rest_b]) do
    case List.keyfind(va,node_b,0) do
      nil-> false
      {_,ctr_a}-> (ctr_a >= ctr_b) and descends(va,rest_b)
    end
  end

  @spec dominates(t, t) :: boolean
  def dominates(a, b), do: descends(a, b) and not descends(b, a)

  @doc """
  subtract the VClock from the DotList.
  what this means is that any `{actor(), count()}' pair in
  DotList that is <= an entry in  VClock is removed from DotList

      iex> subtract_dots([{:a,3},{:b,2},{:d,14},{:g,22}],
      ...>               [{:a,4},{:b,1},{:c,1},{:d,14},{:e,5},{:f,2}])
      [{:b, 2},{:g, 22}]
  """
  @spec subtract_dots(t, t) :: t
  def subtract_dots(dot_list, vclock), do:
    Enum.reject(dot_list,fn {actor,count}-> get_counter(vclock,actor) >= count end)

  @doc "Combine all VClocks in the input list into their least possible common descendant."
  @spec merge(vclocks :: [t]) :: t | []
  def merge([]), do: []
  def merge([single_vclock]), do: single_vclock
  def merge([first|rest]), do: merge(rest, List.keysort(first,0))

  def merge([], nclock), do: nclock
  def merge([aclock|vclocks],nclock), do:
    merge(vclocks, merge(List.keysort(aclock,0), nclock, []))

  def merge([], [], acc_clock), do: Enum.reverse(acc_clock)
  def merge([], left, acc_clock), do: Enum.reverse(acc_clock, left)
  def merge(left, [], acc_clock), do: Enum.reverse(acc_clock, left)
  def merge(v=[{node1, ctr1}=nct1|vclock],n=[{node2,ctr2}=nct2|nclock], acc_clock) do
    case {{node1,node2},{ctr1,ctr2}} do
      {{node1,node2},_} when node1 < node2-> merge(vclock, n, [nct1|acc_clock])
      {{node1,node2},_} when node1 > node2-> merge(v, nclock, [nct2|acc_clock])
      {{node1,node1},{ctr1,ctr2}} when ctr1 > ctr2-> merge(vclock, nclock, [{node1,ctr1}|acc_clock])
      {{node1,node1},{ctr1,ctr2}} when ctr1 < ctr2-> merge(vclock, nclock, [{node1,ctr2}|acc_clock])
      {{node1,node1},{ctr1,ctr1}} -> merge(vclock, nclock, [{node1,ctr1}|acc_clock])
    end
  end

  @doc "Get the counter value in VClock set from Node."
  @spec get_counter(vclock :: t, node :: vclock_node) :: counter
  def get_counter(vclock, node) do
    case List.keyfind(vclock,node,0) do
      nil -> 0
      {_, ctr} -> ctr
    end
  end

  @doc "Increment VClock at Node."
  @spec increment(vclock :: t, node :: vclock_node) :: t
  def increment(vclock, node) do
    {ctr,newv} = case List.keytake(vclock,node,0) do
      nil-> {1, vclock}
      {{_n,c},modv}-> {c+1,modv}
    end
    [{node,ctr}|newv]
  end

  @doc "Return the list of all nodes that have ever incremented VClock."
  @spec all_nodes(vclock :: t) :: [vclock_node]
  def all_nodes(vclock), do: 
    (vclock |> sort |> Enum.map(&elem(&1,0)))

  @doc "Compares two VClocks for equality."
  @spec equal(vclocka :: t, vclockb :: t) :: boolean
  def equal(va,vb), do: Enum.sort(va) === Enum.sort(vb)

  @doc "sorts the vclock by actor"
  @spec sort(t) :: t
  def sort(clock), do: Enum.sort(clock)

  @doc "an effecient format for disk / wire, see: `from_binary/1`"
  @spec to_binary(t) :: binary_vclock
  def to_binary(clock), do: :erlang.term_to_binary(sort(clock))

  @doc "takes the output of `to_binary/1` and returns a vclock"
  @spec from_binary(binary_vclock) :: t
  def from_binary(bin), do: sort(:erlang.binary_to_term(bin))

  @doc "take two vclocks and return a vclock that summerizes only the events both have seen."
  @spec glb(t, t) :: t
  def glb(clock1, clock2) do
    Enum.reduce(clock1,fresh,fn {actor,cnt, glb}->
      case List.keyfind(clock2, actor, 0) do
         nil -> glb
         {^actor,cnt2} when cnt2 >= cnt-> [{actor, cnt} | glb]
         {^actor, cnt2}-> [{actor, cnt2} | glb]
      end
    end) |> Enum.sort
  end
end
