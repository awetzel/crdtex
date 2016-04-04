defmodule Crdtex.Counter do
  @behaviour Crdtex.Behaviour
  @moduledoc """
  # A convergent, replicated, state based PN counter, for embedding in `Crdtex.Map`

  A PN-Counter CRDT. A PN-Counter is essentially two G-Counters:
  one for increments and one for decrements. The value of the counter
  is the difference between the value of the Positive G-Counter and
  the value of the Negative G-Counter. However, this PN-Counter is
  for using embedded in a `Crdtex.Map`. The problem with an embedded
  pn-counter is when the field is removed and added again. PN-Counter
  merge takes the max of P and N as the merged value. In the case
  that a field was removed and re-added P and N maybe be _lower_ than
  their removed values, and when merged with a replica that has not
  seen the remove, the remove is lost. This counter adds some
  causality by storing a `dot' with P and N. Merge takes the max
  event for each actor, so newer values win over old ones. The rest
  of the mechanics are the same.
  
  ## References
  
  - Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) 
    [A comprehensive study of Convergent and Commutative Replicated Data Types.](http://hal.upmc.fr/inria-00555588/)
  """

  defstruct vclock: [], entries: %{}
  @type t :: %Crdtex.Counter{vclock: Vclock.t, entries: 
      %{Crdtex.actor => {event::pos_integer,inc::pos_integer,dec::pos_integer}} }

  @type operation :: increment_op | decrement_op
  @type increment_op :: :increment | {:increment, integer}
  @type decrement_op :: :decrement | {:decrement, integer}

  alias Crdtex.Vclock
  
  @doc "Create a new counter to 0"
  def new, do: %__MODULE__{vclock: Vclock.fresh}

  alias Crdtex.Counter, as: C

  defimpl Crdtex, for: Crdtex.Counter do
    @doc """
    embedded CRDTs most share a causal context with their parent
    Map, setting the internal clock to the parent clock ensures this
    """
    @spec parent_clock(C.t,Vclock.t) :: C.T
    def parent_clock(counter, clock), do: %{counter| vclock: clock}

    @doc "the current integer value of the counter"
    @spec value(C.t) :: integer
    def value(counter) do
      for {_act, {_ev,inc,dec}}<-counter.entries do
        inc - dec
      end |> Enum.sum
    end

    @doc "query value not impl"
    @spec value(C.t,term) :: integer
    def value(counter,_), do: value(counter)

    @doc """
    increment/decrement the counter. Op is either a two tuple of
    `{increment, By}', `{decrement, By}' where `By' is a positive
    integer. Or simply the atoms `increment' or `decrement', which are
    equivalent to `{increment | decrement, 1}' Returns the updated
    counter.
    
    Note: the second argument must be a `riak_dt:dot()', that is a
    2-tuple of `{Actor :: term(), Event :: pos_integer()}' as this is
    for embedding in a `riak_dt_map'
    """
    @spec update(C.t,Crdtex.dot,C.op) :: {:ok, C.t}
    def update(counter,{actor,evt}=dot,op) do
      clock = Vclock.merge([[dot], counter.vclock])
      {_,p,n} = counter.entries[actor] || {nil,0,0}
      {inc, dec} = op({p,n},op)
      {:ok, %{counter| vclock: clock, entries: 
        Dict.put(counter.entries,actor,{evt,inc,dec})}}
    end

    @doc "update with a context. Contexts have no effect. Same as `update/3`"
    @spec update(C.t,Vclock.t,Crdtex.dot,C.op) :: {:ok, C.t}
    def update(counter,_ctx,dot,op), do: update(counter,dot,op)

    #perform the operation `Op' on the {positive, negative} pair for an actor.
    @spec op({p::non_neg_integer, n::non_neg_integer()},C.op) :: {p::non_neg_integer, n::non_neg_integer}
    def op({p,n},:increment), do: op({p,n},{:increment, 1})
    def op({p,n},:decrement), do: op({p,n},{:decrement, 1})
    def op({p,n},{_,0}), do: {p,n}
    def op({p,n},{:increment,by}) when by > 0, do: {p+by,n}
    def op({p,n},{:increment,by}) when by < 0, do: op({p,n},{:decrement,-by})
    def op({p,n},{:decrement,by}) when by > 0, do: {p,n+by}
    def op({p,n},{:decrement,by}) when by < 0, do: op({p,n},{:increment,-by})

    @doc """
    takes two `Crdtex.Counter.t`s and merges them into a single
    `Crdtex.Counter.t`. This is the Least Upper Bound of the Semi-Lattice/CRDT
    literature. The semantics of the `Crdtex.Counter.t` merge are explained in
    the module docs. In a nutshell, merges version vectors, and keeps
    only dots that are present on both sides, or concurrent.
    """
    @spec merge(C.t, C.t) :: C.t
    def merge(cnt, cnt), do: cnt
    def merge(counter1, counter2) do
      clock = Vclock.merge([counter1.vclock, counter2.vclock])
      {entries, unique_entries} = merge_left(counter2.vclock,counter1.entries,counter2.entries)
      entries = merge_right(counter1.vclock, unique_entries, entries)
      %C{vclock: clock, entries: entries}
    end

    #merge the left handside counter (A) by filtering out the dots that
    #are unique to it, and dominated. Returns `[entry()]' as an
    #accumulator, and the dots that are unique to the right hand side (B).
    @spec merge_left(Vclock.t,entries1::%{},entries2::%{}) :: {acc :: %{}, unique :: %{}}
    def merge_left(clock2,entries1,entries2) do
      Enum.reduce(entries1,{%{},entries2}, fn {actor,{evt,_inc,_dec}=cnt}, {keep, cnt2unique}->
        case entries2[actor] do
          nil-> if Vclock.descends(clock2,[{actor, evt}]) do
            {keep,cnt2unique}
          else
            {Dict.put(keep,actor,cnt),cnt2unique}
          end
          {e2,i,d} when e2 > evt-> # RHS has this actor, with a greater dot
            {Dict.put(keep,actor,{e2,i,d}), Dict.delete(cnt2unique, actor)}
          _-> # counter2 has this actor, but a lesser or equal dot
            {Dict.put(keep,actor,cnt), Dict.delete(cnt2unique, actor)}
        end
      end)
    end

    # merge the unique actor entries from the right hand side,
    # keeping the concurrent ones, and dropping the dominated.
    @spec merge_right(Vclock.t, uniques :: %{}, acc :: %{}) :: acc :: %{}
    def merge_right(clock1, uniques, acc) do
      Enum.reduce(uniques,acc,fn {actor,{evt,_,_}=cnt}, keep->
        if Vclock.descends(clock1, [{actor, evt}]) do 
          keep
        else
          Dict.put(keep,actor,cnt)
        end
      end)
    end

    @doc "equality of two counters internal structure, not the `value/1' they produce."
    @spec equal(C.t, C.t) :: boolean
    def equal(counter1, counter2) do
      Vclock.equal(counter1.vclock, counter2.vclock) and
      counter1.entries == counter2.entries
    end

    @doc "generate stats for this counter. Only `actor_count' is produced at present."
    @spec stats(C.t) :: [{:actor_count, pos_integer}]
    def stats(counter), do:
      [{:actor_count, stat(counter,:actor_count)}]

    @doc """
    generate stat for requested stat type at first argument. Only
    `actor_count' is supported at present.  Return a `pos_integer()' for
    the stat requested, or `undefined' if stat type is unsupported.
    """
    @spec stat(C.t,atom) :: pos_integer | nil
    def stat(counter, :actor_count), do: length(counter.vclock)
    def stat(_, _), do: nil
  end
end
