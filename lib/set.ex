defmodule Crdtex.Set do
  @moduledoc """
  riak_dt_orswot: Tombstone-less, replicated, state based observe remove set
  
  An OR-Set CRDT. An OR-Set allows the adding, and removal, of
  elements. Should an add and remove be concurrent, the add wins. In
  this implementation there is a version vector for the whole set.
  When an element is added to the set, the version vector is
  incremented and the `{actor, count}' pair for that increment is
  stored against the element as its "birth dot". Every time the
  element is re-added to the set, its "birth dot" is updated to that
  of the `{actor, count}' version vector entry resulting from the
  add. When an element is removed, we simply drop it, no tombstones.
  
  When an element exists in replica A and not replica B, is it
  because A added it and B has not yet seen that, or that B removed
  it and A has not yet seen that? Usually the presence of a tombstone
  arbitrates. In this implementation we compare the "birth dot" of
  the present element to the clock in the Set it is absent from. If
  the element dot is not "seen" by the Set clock, that means the
  other set has yet to see this add, and the item is in the merged
  Set. If the Set clock dominates the dot, that means the other Set
  has removed this element already, and the item is not in the merged
  Set.
  
  Essentially we've made a dotted version vector.
  
  ## References

  - Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski
    (2011) [A comprehensive study of Convergent and Commutative
    Replicated Data Types](http://hal.upmc.fr/inria-00555588/)
  - Annette Bieniusa, Marek Zawirski, Nuno Preguiça, Marc
    Shapiro, Carlos Baquero, Valter Balegas, Sérgio Duarte (2012) 
    [An Optimized Conﬂict-free Replicated Set](http://arxiv.org/abs/1210.3368)
  - Nuno Preguiça, Carlos Baquero, Paulo Sérgio Almeida,
    Victor Fonte, Ricardo Gonçalves [http://arxiv.org/abs/1011.5808](http://arxiv.org/abs/1011.5808)
  """
  @behaviour Crdtex.Behaviour
  alias Crdtex.Set
  alias Crdtex.Vclock

  defstruct vclock: [], entries: %{}, deferred: %{}
  @type t :: %Crdtex.Set{vclock: Vclock.t,entries: entries, deferred: deferred}

  @type deferred :: %{Vclock.t => MapSet.t}
  @type orswot_op ::  {:add, member} | {:remove, member} |
                      {:add_all, [member]} | {:remove_all, [member]} |
                      {:update, [orswot_op]}
  @type orswot_q  :: :size | {:contains, term}

  @type actor :: Crdtex.actor

  @typedoc """
   a dict of member -> minimal_clock mappings.  The
   `minimal_clock' is a more effecient way of storing knowledge
   about adds / removes than a UUID per add
  """
  @type entries :: %{member: [Crdtex.dot]}
  @type member :: term
  @type precondition_error :: {:error, {:precondition ,{:not_present, member}}}

  @spec new :: Set
  def new, do: %__MODULE__{vclock: Vclock.fresh, entries: %{}, deferred: %{}}

  defimpl Crdtex, for: Crdtex.Set do
    @doc "sets the clock in the Set to that `Clock'. Used by a containing Map for sub-CRDTs"
    @spec parent_clock(Set.t,Vclock.t) :: Set.t
    def parent_clock(set,clock), do: %{set| vclock: clock}
    
    @spec value(Set.t) :: [Set.member]
    def value(%{entries: entries}) do
      entries |> Dict.keys
    end
    
    @spec value(Set.t, Set.orswot_q) :: term
    def value(set,:size), do: length(value(set))
    def value(set,{:contains, e}), do:  
      Enum.member?(value(set), e)
    
    @doc """
    take a list of Set operations and apply them to the set.

    **NOTE**: either _all_ are applied, or _none_ are.
    """
    @spec update(Set.t,Crdtex.actor | Crdtex.dot,Set.dotorswot_op) :: {:ok, Set.t} | Set.precondition_error
    def update(set,actor,{:update, ops}), do:
      apply_ops(set,actor,ops)
    def update(set,actor,{:add, elem}), do:
      {:ok, add_elem(set,actor,elem)}
    def update(set,_actor,{:remove, elem}), do:
      remove_elem(set,set.entries[elem],elem)
    def update(set,actor,{:add_all, elems}), do:
      {:ok,Enum.reduce(elems,set,&add_elem(&2,actor,&1))}

    def update(set,actor,{:remove_all, elems}), do:
      remove_all(set,actor,elems)
    
    @spec update(Set.t,Crdtex.context,Crdtex.actor | Crdtex.dot,Set.orswot_op) :: {:ok, Set.t} | Set.precondition_error
    def update(set,nil,actor,op), do:
      update(set, actor, op)
    def update(set,_ctx,actor,{:add, elem}), do:
      {:ok, add_elem(set,actor,elem)}
    def update(set,ctx,_actor,{:remove, elem}) do
      # Being asked to remove something with a context.  If we
      # have this element, we can drop any dots it has that the
      # Context has seen.
      deferred = defer_remove(set.deferred,set.vclock,ctx,elem)
      case set.entries[elem] do
        nil->
          # Do we not have the element because we removed it already, or
          # because we haven't seen the add?  Should there be a precondition
          # error if Clock descends Ctx?  In a way it makes no sense to have a
          # precon error here, as the precon has been satisfied or will be:
          # this is either deferred or a NO-OP
          {:ok, %{set| deferred: deferred}}
        elem_clock->
          case Vclock.subtract_dots(elem_clock, ctx) do
            []->
              {:ok,%{set| deferred: deferred, entries: Dict.delete(set.entries,elem)}}
            elem_clock->
              {:ok,%{set| deferred: deferred, entries: Dict.put(set.entries,elem,elem_clock)}}
        end
      end
    end

    def update(set,ctx,actor,{:update, ops}), do:
      {:ok,Enum.reduce(ops,set,&elem(update(&2,ctx,actor,&1),1))}
    def update(set,ctx,actor,{:remove_all, elems}), do:
      remove_all(set,ctx,actor,elems)
    def update(set,_ctx,actor,{:add_all, elems}), do:
      update(set,actor,{:add_all, elems})
    
    # If we're asked to remove something we don't have (or have,
    # but maybe not having seen all 'adds' for the element), is it
    # because we've not seen the add that we've been asked to remove, or
    # is it because we already removed it? In the former case, we can
    # "defer" this operation by storing it, with its context, for later
    # execution. If the clock for the Set descends the operation clock,
    # then we don't need to defer the op, its already been done. It is
    # _very_ important to note, that only _actorless_ operations can be
    # saved. That is operations that DO NOT increment the clock. In
    # ORSWOTS this is easy, as only removes need a context. A context for
    # an 'add' is meaningless.
    #
    # @TODO revist this, as it might be meaningful in some cases (for
    # true idempotence) and we can have merges triggering updates,
    # maybe.)
    @spec defer_remove(Set.deferred,Vclock.t,Vclock.t, Set.member) :: Set.deferred
    def defer_remove(deferred,clock, ctx, elem) do
      if Vclock.descends(clock,ctx) do
        deferred
      else
        Dict.update(deferred,ctx,MapSet.new([elem]), &MapSet.put(&1,elem))
      end
    end
    
    @spec apply_ops(Set.t,Crdtex.actor | Crdtex.dot,[Set.orswot_op]) :: {:ok,Set.t} | Set.precondition_error
    def apply_ops(set,_actor,[]), do: {:ok,set}
    def apply_ops(set,actor,[op | rest]) do
      case update(set,actor,op) do
        {:ok, set}-> apply_ops(set,actor,rest)
        error->error
      end
    end
    
    def remove_all(set,_actor,[]), do: {:ok, set}
    def remove_all(set, actor,[elem | rest]) do
      case update(set,actor,{:remove, elem}) do
        {:ok, set}-> remove_all(set,actor,rest)
        error-> error
      end
    end
    
    def remove_all(set,_ctx,_actor,[]), do: {:ok, set}
    def remove_all(set, ctx, actor, [elem | rest]) do
      {:ok, set} = update(set,ctx,actor,{:remove, elem})
      remove_all(set,ctx,actor,rest)
    end
    
    @spec merge(Set.t, Set.t) :: Set.t
    def merge(set1, set1), do: set1
    def merge(set1,set2) do
      clock = Vclock.merge([set1.vclock,set2.vclock])
      {keep,entries2} = Enum.reduce(set1.entries,{%{},set2.entries}, fn {e,dots}, {acc,remaining2}->
        case set2.entries[e] do
          nil-> # Only on left, trim dots and keep surviving
            case Vclock.subtract_dots(dots, set2.vclock) do
              []-> {acc,remaining2} #removed
              newdots->{Dict.put(acc,e,newdots),remaining2}
            end
          dots2-> # On both sides
            dots_set = MapSet.new(dots); dots2_set = MapSet.new(dots2)
            common = MapSet.intersection(dots_set,dots2_set)
            unique1 = MapSet.difference(dots_set,common)
            unique2 = MapSet.difference(dots2_set,common)
            keep1 = Vclock.subtract_dots(unique1, set2.vclock)
            keep2 = Vclock.subtract_dots(unique2, set1.vclock)
            case Vclock.merge([Enum.to_list(common), Enum.to_list(keep1), Enum.to_list(keep2)]) do
              []-> #removed from both sides
                {acc, Dict.delete(remaining2,e)}
              v-> {Dict.put(acc,e,v),Dict.delete(remaining2,e)}
            end
        end
      end)
      # Now what about the stuff left from the right hand side? Do the same to that!
      entries = Enum.reduce(entries2,keep,fn {e,dots}, acc->
        case Vclock.subtract_dots(dots,set1.vclock) do
          [] -> acc
          newdots-> Dict.put(acc,e,newdots)
        end
      end)
      deferred = merge_deferred(set1.deferred, set2.deferred)
      apply_deferred(clock, entries, deferred)
    end
    
    # merge the deffered operations for both sets.
    @spec merge_deferred(Set.deferred, Set.deferred) :: Set.deferred
    defp merge_deferred(deferred1, deferred2) do
      Dict.merge(deferred1,deferred2,fn _k,v1,v2->MapSet.union(v1,v2) end)
    end

    # any operation in the deferred list that has been seen as a
    # result of the merge, can be applied
    #
    # @TODO again, think hard on this, should it be called in process by
    # an actor only?
    @spec apply_deferred(Vclock.t, Set.entries, Set.deferred) :: Set.t
    def apply_deferred(clock, entries, deferred) do
      Enum.reduce(deferred,%Set{vclock: clock, entries: entries, deferred: %{}}, fn {ctx,elems}, set->
        {:ok,set} = remove_all(set,ctx,nil,elems)
        set
      end)
    end
    
    @spec equal(Set.t, Set.t) :: boolean
    def equal(set1, set2) do
      Vclock.equal(set1.vclock, set2.vclock) and
        Enum.sort(Dict.keys(set1.entries)) == Enum.sort(Dict.keys(set2.entries)) and
        dots_equal(set1.entries, set2.entries)
    end
    
    @spec dots_equal(%{Set.member => [Crdtex.dot]}, %{Set.member => [Crdtex.dot]}) :: boolean
    def dots_equal(entries1,entries2) do
      Enum.all?(entries1, fn {e,clock1}->
        Vclock.equal(clock1,entries2[e])
      end)
    end
    
    @spec add_elem(Set.t, Crdtex.actor | Crdtex.dot, Set.member) :: Set.t
    defp add_elem(set,dot,elem) when is_tuple(dot), do:
      %{set| vclock: Vclock.merge([set.vclock, [dot]]), entries: Dict.put(set.entries,elem,[dot])}
    defp add_elem(set,actor,elem) do
      new_clock = Vclock.increment(set.vclock,actor)
      dot = [{actor,Vclock.get_counter(new_clock,actor)}]
      %{set| vclock: new_clock, entries: Dict.put(set.entries,elem,dot)}
    end
    
    @spec remove_elem(Set.t,nil | Vclock.t,Set.member) :: {:ok,Set.t} | Set.precondition_error
    def remove_elem(_set, nil, elem), do:
      {:error, {:precondition, {:not_present, elem}}}
    def remove_elem(set,_vclock, elem), do:
      {:ok, %{set| entries: Dict.delete(set.entries,elem)}}
    
    @doc """
    the precondition context is a fragment of the CRDT that
    operations requiring certain pre-conditions can be applied with.
    Especially useful for hybrid op/state systems where the context of
    an operation is needed at a replica without sending the entire
    state to the client. In the case of the ORSWOT the context is a
    version vector. When passed as an argument to `update/4' the
    context ensures that only seen adds are removed, and that removes
    of unseen adds can be deferred until they're seen.
    """
    @spec precondition_context(Set.t) :: Set.t
    def precondition_context(set), do: set.vclock
    
    @spec stats(Set.t) :: [{atom, number}]
    def stats(set) do
      for prop<-[:actor_count,:element_count,:max_dot_length,:deferred_length] do
        {prop,stat(set,prop)}
      end
    end
    
    @spec stat(Set.t,atom) :: number | nil
    def stat(set,:actor_count), do: length(set.vclock)
    def stat(set,:element_count), do: map_size(set.entries)
    def stat(set,:deferred_length), do: map_size(set.deferred)
    def stat(set,:max_dot_length) do
      Enum.reduce(set.entries,0,fn {_k,dots}, acc->
        max(length(dots), acc)
      end)
    end
    def stat(_,_), do: nil
  end
end
