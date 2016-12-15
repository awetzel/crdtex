defmodule Crdtex.Map do
  @moduledoc """
  OR-Set schema based multi CRDT container
  A multi CRDT holder. A Struct/Document-ish thing. Uses the
  same tombstone-less, Observed Remove semantics as `riak_dt_orswot`.
  A Map is set of `Field's a `Field' is a two-tuple of:
  `{Name::binary(), CRDTModule::module()}' where the second element
  is the name of a crdt module that may be embedded. CRDTs stored
  inside the Map will have their `update/3/4' function called, but the
  second argument will be a `riak_dt:dot()', so that they share the
  causal context of the map, even when fields are removed, and
  subsequently re-added.
  
  The contents of the Map are modeled as a dictionary of
  `field_name()' to `field_value()' mappings. Where `field_ name()'
  is a two tuple of an opaque `binary()' name, and one of the
  embeddable crdt types (currently `riak_dt_orswot',
  `riak_dt_emcntr', `riak_dt_lwwreg', `riak_dt_od_flag', and
  `riak_dt_map'). The reason for this limitation is that embedded
  types must support embedding: that is a shared, `dot'-based, causal
  context, and a reset-remove semantic (more on these below.)  The
  `field_value()' is a two-tuple of `entries()' and a
  `tombstone()'. The presence of a `tombstone()' in a "tombstoneless"
  Map is confusing. The `tombstone()' is only stored for fields that
  are currently in the map, removing a field also removes its
  tombstone.
  
  To use the Map create a `new()' Map. When you call `update/3' or
  `update/4' you pass a list of operations and an optional causal
  context. @See `update/3' or `update/4' for more details. The list
  of operations is applied atomically in full, and new state
  returned, or not at all, and an error is returned.
  
  ## Semantics ##
  
  The semantics of this Map are Observed-Remove-Reset-Remove. What
  this means is practice is, if a field is removed, and concurrently
  that same field is updated, the field is _in_ the Map (only
  observed updates are removed) but those removes propagate, so only
  the concurrent update survives. A concrete example helps: If a Map
  contains a field that is a set, and the set has 5 elements in it,
  and concurrently the replica at A removes the field that contains
  the set, while the replica at B adds an item to the set, on merge
  there is a field for the set, but it contains only the one item B
  added. The removal of the field is semantically equivalent to
  removing all elements in the set, and removing the field. The same
  goes for an embedded Map. If concurrently a Map field is removed,
  while a new sub-field is updated, only the updated field(s) survive
  the reset-remove.
  
  There is an anomaly for embedded counters that does not fully
  support reset remove. Embedded counters (@see riak_dt_emcntr) are
  different to a normal `pn-counter'. Embedded counters map `dot's to
  {P, N} pairs. When a counter is incremented a new dot is created,
  that replaces the old dot with the new value. `pn-counter' usually
  merges by taking the `max' of any `P' or `N' entry for an
  actor. This does not work in an embedded context. When a counter
  field is removed, and then _re_-added, the new `P' and `N' entries
  may be lower than the old, and merging loses the remove
  information. However, if a `dot' is stored with the value, and the
  max of the `dot' is used in merge, new updates win over removed
  updates. So far so good. Here is the problem. If Replica B removes
  a counter field, and does not re-add it, and replica A concurrently
  updates it's entry for that field, then the reset-remove does not
  occur. All new dots are not `observed' by Replica B, so not
  removed. The new `dots' contain the updates from the previous
  `dots', and the old `dot' is discarded. To achieve reset-remove all
  increments would need a dot, and need to be retained, which would
  be very costly in terms of space. One way to accept this anomaly is
  to think of a Map like a file system: removing a directory and
  concurrently adding a file means that the directory is present and
  only the file remains in it. Updating a counter and concurrently
  removing it, means the counter remains, with the updated value,
  much like appending to a file in the file system analogy: you don't
  expect only the diff to survive, but the whole updated file.
  
  ## Merging/Size ##
  
  When any pair of Maps are merged, the embedded CRDTs are _not_
  merged, instead each concurrent `dot'->`field()' entry is
  kept. This leads to a greater size for Maps that are highly
  divergent. Updating a field in the map, however, leads to all
  entries for that field being merged to a single CRDT that is stored
  against the new `dot'. As mentioned above, there is also a
  `tombstone' entry per present field. This is bottom CRDT for the
  field type with a clock that contains all seen and removed
  `dots'. There tombstones are merged at merge time, so only one is
  present per field. Clearly the repetition of actor information (the
  clock, each embedded CRDT, the field `dots', the tombstones) is a
  serious issue with regard to size/bloat of this data type. We use
  erlang's `to_binary/2' function, which compresses the data, to get
  around this at present.
  
  ## Context and Deferred operations ##
  
  For CRDTs that use version vectors and dots (this `Map' and all
  CRDTs that may be embedded in it), the size of the CRDT is
  influenced by the number of actors updating it. In some systems
  (like Riak!) we attempt to minimize the number of actors by only
  having the database update CRDTs. This leads to a kind of "action
  at a distance", where a client sends operations to the database,
  and an actor in the database system performs the operations. The
  purpose is to ship minimal state between database and client, and
  to limit the number of actors in the system. There is a problem
  with action at a distance and the OR semantic. The client _must_ be
  able to tell the database what has been observed when it sends a
  remove operation. There is a further problem. A replica that
  handles an operation may not have all the state the client
  observed. We solve these two problems by asking the client to
  provide a causal context for operations (@see `update/4'.) Context
  operations solve the OR problem, but they don't solve the problem
  of lagging replicas handling operations.
  
  ### Lagging replicas, deferred operations ###
  
  In a system like Riak, a replica that is not up-to-date (including,
  never seen any state for a CRDT) maybe asked to perform an
  operation. If no context is given, and the operation is a field
  remove, or a "remove" like operation on an embedded CRDT, the
  operation may fail with a precondition error (for example, remove a
  field that is not present) or succeed and remove more state than
  intended (a field remove with no context may remove updates unseen
  by the client.) When a context is provided, and the Field to be
  removed is absent, the Map state stores the context, and Field
  name, in a list of deferred operations. When, eventually, through
  propagation and merging, the Map's clock descends the context for
  the operation, the operation is executed. It is important to note
  that _only_ actorless (field remove) operations can occur this way.
  
  #### Embedded CRDTs Deferred Operations ####
  
  There is a bug with embedded types and deferred operations. Imagine
  a client has seen a Map with a Set field, and the set contains {a,
  b, c}. The client sends an operation to remove {a} from the set. A
  replica that is new takes the operation. It will create a new Map,
  a Field for the Set, and store the `remove` operation as part of
  the Set's state. A client reads this new state, and sends a field
  remove operation, that is executed by same replica. Now the
  deferred operation is lost, since the field is removed. We're
  working on ways to fix this. One idea is to not remove a field with
  "undelivered" operations, but instead to "hide" it.
  
  See {@link riak_dt_orswot} for more on the OR semantic
  
  See {@link riak_dt_emcntr} for the embedded counter.
  """
  @behaviour Crdtex

  defstruct vclock: [], entries: %{}, deferred: %{}
  @type t :: %Crdtex.Map{vclock: Vclock.t, entries: entries, deferred: deferred}

  @typedoc "A binary that from_binary/1 will accept"
  @type binary_map :: binary
  @type entries :: %{field_name => field_value}
  @type field :: {field_name, field_value}
  @type field_name :: binary
  @type field_value :: {crdts, tombstone}

  @type crdts :: [entry]
  @type entry :: {Crdtex.dot, Crdtex.t}

  @typedoc "Only for present fields, ensures removes propagation"
  @type tombstone :: Crdtex.t

  @typedoc """
  Only field removals can be deferred. CRDTs stored in the map may
  have contexts and deferred operations, but as these are part of the
  state, they are stored under the field as an update like any other.
  """
  @type deferred :: %{context=> [field]}

  @type map_op :: {:update, [map_field_update | map_field_op]}

  @type map_field_op ::  {:remove, field}
  @type map_field_update :: {:update, field, Crdtex.operation}

  alias Crdtex.Vclock
  @type context :: Vclock.t | nil
  @type value :: {field, [value] | integer | [term] | boolean | term}
  @type precondition_error :: {:error, {:precondition, {:not_present, field}}}


  @doc "Create a new, empty Map."
  @spec new() :: t
  def new, do:
    %__MODULE__{vclock: Vclock.fresh, deferred: %{}, entries: %{}}

  defimpl Crdtex, for: Crdtex.Map do
    alias Crdtex.Map, as: M

    @doc "sets the clock in the map to that `Clock'. Used by a containing Map for sub-CRDTs"
    def parent_clock(map, clock), do: %{map| vclock: clock}

    @doc "get the current set of values for this Map"
    @spec value(M.t) :: [M.value]
    def value(%{entries: entries}) do
      Enum.map(entries,fn {name,crdts}->
        {name,crdts |> merge_crdts |> Crdtex.value}
      end)
    end

    def value(map,_), do: value(map)

    ## @private merge the CRDTs of a type
    defp merge_crdts({crdts, ts}) do
      v = Enum.reduce(crdts, ts.__struct__.new, fn {_dot,crdt},acc-> 
        Crdtex.merge(acc,crdt)
      end)
      # Merge with the tombstone to drop any removed dots
      Crdtex.merge(ts, v)
    end

    @doc """
    update the `riak_dt_map()' or a field in the `riak_dt_map()' by
    executing the `map_op()'. `Ops' is a list of one or more of the
    following ops:
  
    `{update, field(), Op} where `Op' is a valid update operation for a
    CRDT of type `Mod' from the `Key' pair `{Name, Mod}' If there is no
    local value for `Key' a new CRDT is created, the operation applied
    and the result inserted otherwise, the operation is applied to the
    local value.
  
     `{remove, `field()'}' where field is `{name, type}', results in
     the crdt at `field' and the key and value being removed. A
     concurrent `update' will "win" over a remove so that the field is
     still present, and it's value will contain the concurrent update.
  
    Atomic, all of `Ops' are performed successfully, or none are.

    the context ensures no unseen field updates are removed, and removal of
    unseen updates is deferred. The Context is passed down as the context for
    any nested types. hence the common clock.
    """
    @spec update(M.t, Crdtex.context, Crdtex.actor | Crdtex.dot, M.map_op) :: {:ok, M.t} | M.precondition_error
    def update(%{entries: _entries,vclock: clock}=map, ctx \\ nil, actor_or_dot, {:update,ops}) do
      {dot, clock} = update_clock(actor_or_dot, clock)
      apply_ops(%{map|vclock: clock},ctx,dot,ops)
    end

    @spec apply_ops(M.t,M.context,Crdtex.dot,[M.map_field_update | M.map_field_op]) :: {:ok,M.t} | M.precondition_error
    def apply_ops(map,_ctx,_dot,[]), do: {:ok, map}
    def apply_ops(map,ctx,dot,[{:update, name, op}| rest]), do: apply_ops(map,ctx,dot,[{:update, name, op, nil}| rest])
    def apply_ops(map,ctx,dot,[{:update, name, op, lazy_default}| rest]) do
      case ((crdts=map.entries[name]) && merge_crdts(crdts)) || (lazy_default && lazy_default.()) do
        nil-> {:error, {:precondition, {:not_present, name}}}
        crdt->
          crdt = Crdtex.parent_clock(crdt, map.vclock)
          case Crdtex.update(crdt,ctx,dot,op) do
            {:ok,updated}-> 
              new_entries = put_in(map.entries,[name],{%{dot=>updated},updated.__struct__.new}) # old tombstone was merged so empty one 
              apply_ops(%{map|entries: new_entries},ctx,dot,rest)
            error -> error
          end
      end
    end

    def apply_ops(map,ctx,dot,[{:remove, name} | rest]) do
      case remove_field(map,ctx,name) do
        {:ok, map} -> apply_ops(map,ctx,dot,rest)
        e -> e
      end
    end

    # @private update the clock, and get a dot for the operations. This
    # means that field removals increment the clock too.
    @spec update_clock(Crdtex.actor | Crdtex.dot, Vclock.t) :: {Crdtex.dot, Vclock.t}
    defp update_clock(dot, clock) when is_tuple(dot), do: 
      {dot,Vclock.merge([[dot], clock])}
    defp update_clock(actor,clock) do
      clock = Vclock.increment(clock,actor)
      {{actor,Vclock.get_counter(clock,actor)},clock}
    end

    # @private when context is undefined, we simply remove all instances
    # of Field, regardless of their dot. If the field is not present then
    # we warn the user with a precondition error. However, in the case
    # that a context is provided we can be more fine grained, and only
    # remove those field entries whose dots are seen by the context. This
    # preserves the "observed" part of "observed-remove". There is no
    # precondition error if we're asked to remove smoething that isn't
    # present, either we defer it, or it has been done already, depending
    # on if the Map clock descends the context clock or not.
    # 
    # {@link defer_remove/4} for handling of removes of fields that are
    # _not_ present
    @spec remove_field(M.t, M.context, M.field) :: {:ok, M.t} | M.precondition_error
    defp remove_field(map,nil,name) do
      if Dict.has_key?(map.entries,name) do
        {:ok,%{map| entries: Dict.delete(map.entries,name)}}
      else
        {:error, {:precondition, {:not_present, name}}}
      end
    end

    # Context removes
    defp remove_field(map,ctx,name) do
      deferred = defer_remove(map,ctx,name)
      entries = case ctx_rem_field(map,ctx,map.entries[name]) do
        nil-> Dict.delete(map.entries,name)
        crdts-> Dict.put(map.entries,name,crdts)
      end
      {:ok, %{map| entries: entries, deferred: deferred}}
    end

    # drop dominated fields
    defp ctx_rem_field(_map,_ctx, nil), do: nil
    defp ctx_rem_field(map,ctx, {crdts, prev_tombstone, _type}) do
      # Drop dominated fields, and update the tombstone.
      #
      # If the context is removing a field at dot {a, 1} and the
      # current field is {a, 2}, the tombstone ensures that all events
      # from {a, 1} are removed from the crdt value. If the ctx remove
      # is at {a, 3} and the current field is at {a, 2} then we need to
      # remove only events upto {a, 2}. The glb clock enables that.

      # GLB is events seen by both clocks only
      new_tombstone  = Crdtex.parent_clock(prev_tombstone.__struct__.new,Vclock.glb(ctx, map.clock))
      case crdts |> Enum.filter(fn {dot,_crdt}-> is_dot_unseen(dot,ctx) end) |> Enum.into(%{}) do
        remaining when map_size(remaining) == 0 -> nil # Ctx remove removed all dots for field
        remaining->{remaining, Crdtex.merge(prev_tombstone,new_tombstone)}
      end
    end
      
    # If we're asked to remove something we don't have (or have,
    # but maybe not all 'updates' for it), is it because we've not seen
    # the some update that we've been asked to remove, or is it because
    # we already removed it? In the former case, we can "defer" this
    # operation by storing it, with its context, for later execution. If
    # the clock for the Map descends the operation clock, then we don't
    # need to defer the op, its already been done. It is _very_ important
    # to note, that only _actorless_ operations can be saved. That is
    # operations that DO NOT need to increment the clock. In a Map this
    # means field removals only. Contexts for update operations do not
    # result in deferred operations on the parent Map. This simulates
    # causal delivery, in that an `update' must be seen before it can be
    # `removed'.
    @spec defer_remove(M.t,Vclock.t, M.field) :: M.deferred
    defp defer_remove(map,ctx,name) do
      if Vclock.descends(map.vclock,ctx) do map.deferred else
        Dict.update(map.deferred,ctx,MapSet.new([name]),& MapSet.put(&1,name))
      end
    end

    @doc "merge two `riak_dt_map()'s."
    @spec merge(M.t, M.t) :: M.t
    def merge(map, map), do: map

    ## @TODO is there a way to optimise this, based on clocks maybe?
    def merge(map1, map2) do
      clock = Vclock.merge([map1.vclock, map2.vclock])
      {common, map1_unique, map2_unique} = key_sets(map1.entries, map2.entries)
      entries = %{}
        |> filter_unique(map1_unique,map1.entries,map2.vclock)
        |> filter_unique(map2_unique,map2.entries,map1.vclock)
        |> merge_common(map1.entries,map2.entries,map1.vclock,map2.vclock, common)
      deferred = merge_deferred(map2.deferred, map1.deferred)
      apply_deferred(clock,entries,deferred)
    end

    # filter the set of fields that are on one side of a merge only
    @spec filter_unique(M.entries,MapSet.t, M.entries, Vclock.t) :: M.entries
    defp filter_unique(acc,field_set, entries, clock) do
      Enum.reduce(field_set, acc, fn name, acc->
        {dots, tombstone} = entries[name]
        case dots |> Stream.filter(fn {dot,_crdt}-> is_dot_unseen(dot,clock) end) |> Enum.into(%{}) do
          keep_dots when map_size(keep_dots) == 0->acc
          keep_dots->
            # create a tombstone since the otherside does not have this field,
            # it either removed it, or never had it. If it never had it, the
            # removing dots in the tombstone will have no impact on the value,
            # if the otherside removed it, then the removed dots will be
            # propogated by the tombstone.
            tombstone = Crdtex.merge(tombstone, Crdtex.parent_clock(tombstone.__struct__.new, clock))
            Dict.put(acc, name, {keep_dots, tombstone})
        end
      end)
    end

    # predicate function, `true' if the provided `dot()' is concurrent with the
    # clock, `false' if the clock has seen the dot.
    @spec is_dot_unseen(Crdtex.dot, Vclock.t) :: boolean
    defp is_dot_unseen(dot, clock), do: not Vclock.descends(clock, [dot])

    # Get the keys from an ?DICT as a ?SET
    defp key_set(dict), do: (dict |> Dict.keys |> Enum.into(MapSet.new))

    @doc """
    break the keys from an two ?DICTs out into three ?SETs, the
    common keys, those unique to one, and those unique to the other."
    """
    @spec key_sets(%{}, %{}) :: {MapSet.t, MapSet.t, MapSet.t}
    def key_sets(entries1, entries2) do
      entries_set1 = key_set(entries1); entries_set2 = key_set(entries2)
      {MapSet.intersection(entries_set1,entries_set2),
       MapSet.difference(entries_set1,entries_set2),
       MapSet.difference(entries_set2,entries_set1)}
    end

    # for a set of dots (that are unique to one side) decide whether
    # to keep, or drop each.
    @spec filter_dots(MapSet.t, %{}, Vclock.t) :: M.entries
    defp filter_dots(dots, crdts, clock) do
      to_keep = dots |> Stream.filter(&is_dot_unseen(&1,clock)) |> Enum.into(MapSet.new)
      crdts |> Stream.filter(fn {dot,_}->MapSet.member?(to_keep,dot) end) |> Enum.into(%{})
    end

    # merge the common fields into a set of surviving dots and a
    # tombstone per field.  If a dot is on both sides, keep it. If it
    # is only on one side, drop it if dominated by the otherside's clock.
    def merge_common(acc,entries1, entries2, vclock1, vclock2, common_fields) do
      Enum.reduce(common_fields,acc,fn name, acc->
        {dots1, ts1} = entries1[name]
        {dots2, ts2} = entries2[name]
        {common_dots, unique1, unique2} = key_sets(dots1, dots2)
        ts = Crdtex.merge(ts2, ts1)
        common_surviving = Enum.reduce(common_dots,%{},fn dot, acc->
          Dict.put(acc,dot,dots1[dot])
        end)

        surviving1 = filter_dots(unique1, dots1, vclock2)
        surviving2 = filter_dots(unique2, dots2, vclock1)

        dots = Stream.concat([common_surviving,surviving1,surviving2]) |> Enum.into(%{})
        if map_size(dots) == 0, do: acc, else: Dict.put(acc,name,{dots,ts})
      end)
    end

    @spec merge_deferred(M.deferred, M.deferred) :: M.deferred
    defp merge_deferred(deferred1, deferred2) do
      Map.merge(deferred1,deferred2,fn _k, v1, v2->
        MapSet.union(v1,v2)
      end)
    end

    # apply those deferred field removals, if they're preconditions have been met, that is.
    @spec apply_deferred(Vclock.t, M.entries, M.deferred) :: M.t
    defp apply_deferred(clock, entries, deferred) do
      Enum.reduce(deferred,%M{vclock: clock, entries: entries},fn {ctx,fields},acc->
        remove_all(acc,ctx,fields)
      end)
    end

    @spec remove_all(M.t,M.context,[M.field]) :: M.t
    defp remove_all(map, ctx, fields) do
      Enum.reduce(fields,map,fn field, acc->
        {:ok, acc} = remove_field(acc, ctx, field)
        acc
      end)
    end

    @doc """
    compare two `riak_dt_map()'s for equality of structure Both
    schemas and value list must be equal. Performs a pariwise equals for
    all values in the value lists
    """
    @spec equal(M.t, M.t) :: boolean
    def equal(map1,map2) do 
      Vclock.equal(map1.vclock,map2.vclock) and 
      map1.deferred == map2.deferred and
      pairwise_equals(Enum.sort(map1.entries),Enum.sort(map2.entries))
    end

    @spec pairwise_equals([M.field], [M.field]) :: boolean
    defp pairwise_equals([], []), do: true
    defp pairwise_equals([{name, {dots1, ts1}}| rest1], [{name, {dots2, ts2}}| rest2]) do
      # Tombstones don't need to be equal. When we merge with a map
      # where one side is absent, we take the absent sides clock, when
      # we merge where both sides have a field, we merge the
      # tombstones, and apply deferred. The deferred remove uses a glb
      # of the context and the clock, meaning we get a smaller
      # tombstone. Both are correct when it comes to determining the
      # final value. As long as tombstones are not conflicting (that is
      # A == B | A > B | B > A)
      case {Dict.keys(dots1) == Dict.keys(dots2), Crdtex.equal(ts1,ts2)} do
        {true, true} -> pairwise_equals(rest1, rest2)
        _ -> false
      end
    end
    defp pairwise_equals(_, _), do: false

    @doc """
    an opaque context that can be passed to `update/4' to ensure
    that only seen fields are removed. If a field removal operation has
    a context that the Map has not seen, it will be deferred until
    causally relevant.
    """
    @spec precondition_context(M.t) :: Crdtex.context
    def precondition_context(map), do: map.vclock

    @doc """
    stats on internal state of Map.
    A proplist of `{StatName :: atom(), Value :: integer()}'. Stats exposed are:
    `actor_count': The number of actors in the clock for the Map.
    `field_count': The total number of fields in the Map (including divergent field entries).
    `duplication': The number of duplicate entries in the Map across all fields.
                   basically `field_count' - ( unique fields)
    `deferred_length': How many operations on the deferred list, a reasonable expression
                      of lag/staleness.
    """
    @spec stats(M.t) :: [{atom, integer}]
    def stats(map) do
      for prop<-[:actor_count,:field_count,:duplication,:deferred_length] do
        {prop, stat(map,prop)}
      end
    end

    @spec stat(M.t,atom) :: number | nil
    def stat(map,:actor_count), do: length(map.vclock)
    def stat(map,:field_count), do: map_size(map.entries)
    def stat(map,:duplication) do
     {nb_fields,nb_dup} = Enum.reduce(map.entries,{0,0}, fn {_, {dots ,_}}, {nb_fields, nb_dup}->
       {nb_fields+1, nb_dup + map_size(dots)}
     end)
     nb_dup - nb_fields
    end
    def stat(map,:deferred_length), do: map_size(map.deferred)
    def stat(_map,_), do: nil
  end
end
