defmodule Crdtex.SetTest do
  use ExUnit.Case
  alias Crdtex.Set

  test "stat" do
    set = Set.new
    assert Crdtex.stats(set) == [actor_count: 0, element_count: 0, max_dot_length: 0, deferred_length: 0]

    {:ok,set} = Crdtex.update(set,1,{:add,"foo"})
    {:ok,set} = Crdtex.update(set,2,{:add,"foo"})
    {:ok,set} = Crdtex.update(set,3,{:add,"bar"})
    {:ok,set} = Crdtex.update(set,1,{:remove,"foo"})

    assert Crdtex.stat(set,:actor_count) == 3
    assert Crdtex.stat(set,:element_count) == 1
    assert Crdtex.stat(set,:max_dot_length) == 1
    assert Crdtex.stat(set,:waste_pct) == nil
  end

  test "disjoint merge" do
    # Added by @asonge from github to catch a bug I added by trying to
    # bypass merge if one side's clcok dominated the others. The
    # optimisation was bogus, this test remains in case someone else
    # tries that
    {:ok, a} = Crdtex.update(Set.new,1,{:add,"bar"})
    {:ok, b} = Crdtex.update(Set.new,2,{:add,"baz"})
    c = Crdtex.merge(a,b)
    {:ok, a} = Crdtex.update(a,1,{:remove,"bar"})
    d = Crdtex.merge(a,c)

    assert Crdtex.value(d) == ["baz"]
  end

  test "present but removed" do
    # Bug found by EQC, not dropping dots in merge when an element is
    # present in both Sets leads to removed items remaining after merge.

    # Add Z to A
    {:ok,a} = Crdtex.update(Set.new, :a, {:add, :Z})
    # Replicate it to C so A has 'Z'->{e, 1}
    c = a
    # Remove Z from A
    {:ok,a} = Crdtex.update(a,:a,{:remove, :Z})
    # Add Z to B, a new replica
    {:ok, b} = Crdtex.update(Set.new,:b,{:add, :Z})
    # Replicate B to A, so now A has a Z, the one with a Dot of
    # {b,1} and clock of [{a, 1}, {b, 1}]
    a = Crdtex.merge(b, a)
    # Remove the 'Z' from B replica
    {:ok, b} = Crdtex.update(b,:b,{:remove, :Z})

    # Both C and A have a 'Z', but when they merge, there should be no
    # 'Z' as C's has been removed by A and A's has been removed by C.
    # the order matters, the two replicas that have 'Z' need to merge
    # first to provoke the bug. You end up with 'Z' with two dots,
    # when really it should be removed.
    merged = Enum.reduce([c,b],a,&Crdtex.merge(&1,&2))
    assert [] == Crdtex.value(merged)
  end

  test "no dots left" do
    # A bug EQC found where dropping the dots in merge was not enough if
    # you then store the value with an empty clock (derp).
    {:ok, a} = Crdtex.update(Set.new,:a,{:add, :Z})
    {:ok, b} = Crdtex.update(Set.new,:b,{:add, :Z})
    c = a # replicate A to empty C
    {:ok, a} = Crdtex.update(a,:a,{:remove, :Z})
    # replicate B to A, now A has B's 'Z'
    a = Crdtex.merge(a,b)
    # Remove B's 'Z'
    {:ok, b} = Crdtex.update(b,:b,{:remove, :Z})
    # Replicate C to B, now B has A's old 'Z'
    b = Crdtex.merge(b,c)
    # Merge everytyhing, without the fix You end up with 'Z' present, with no dots
    merged = Enum.reduce([b,c],a,&Crdtex.merge(&1,&2))
    assert [] == Crdtex.value(merged)
  end

  # A test I thought up
  # - existing replica of ['A'] at a and b,
  # - add ['B'] at b, but not communicated to any other nodes, context returned to client
  # - b goes down forever
  # - remove ['A'] at a, using the context the client got from b
  # - will that remove happen?
  #   case for shouldn't: the context at b will always be bigger than that at a
  #   case for should: we have the information in dots that may allow us to realise it can be removed
  #     without us caring.
  #
  # as the code stands, 'A' *is* removed, which is almost certainly correct. This behaviour should
  # always happen, but may not. (ie, the test needs expanding)
  test "dead node update" do
    {:ok, a} = Crdtex.update(Set.new,:a,{:add, :A})
    {:ok, b} = Crdtex.update(a,:b,{:add, :B})
    bctx = Crdtex.impl_for(b).precondition_context(b)
    {:ok, a} = Crdtex.update(a,bctx,:a,{:remove, :A})
    assert [] == Crdtex.value(a)
  end

  #%% Batching should not re-order ops
  test "batch order" do
    {:ok, set} = Crdtex.update(Set.new,:a,{:add_all, ["bar","baz"]})
    ctx  = Crdtex.impl_for(set).precondition_context(set)
    {:ok, set} = Crdtex.update(set,ctx,:a,{:update, remove: "baz", add: "baz"})
    assert ["bar","baz"] == Crdtex.value(set)

    {:ok, set} = Crdtex.update(set,:a,{:update, remove: "baz", add: "baz"})
    assert ["bar","baz"] == Crdtex.value(set)

    {:ok, set} = Crdtex.update(set,:a,{:remove, "baz"})
    {:ok, set} = Crdtex.update(set,:a,{:add, "baz"})
    assert ["bar","baz"] == Crdtex.value(set)
  end

  #-ifdef(EQC).
  #
  #bin_roundtrip_test_() ->
  #    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).
  #
  #eqc_value_test_() ->
  #    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).
  #
  #size(Set) ->
  #    value(size, Set).
  #
  #generate() ->
  #    %% Only generate add ops
  #    ?LET({Ops, Actors}, {non_empty(list({add, bitstring(20*8)})), non_empty(list(bitstring(16*8)))},
  #         lists:foldl(fun(Op, Set) ->
  #                             Actor = case length(Actors) of
  #                                         1 -> hd(Actors);
  #                                         _ -> lists:nth(crypto:rand_uniform(1, length(Actors)), Actors)
  #                                     end,
  #                             case riak_dt_orswot:update(Op, Actor, Set) of
  #                                 {ok, S} -> S;
  #                                 _ -> Set
  #                             end
  #                     end,
  #                     riak_dt_orswot:new(),
  #                     Ops)).
  #
  #%% EQC generator
  #gen_op() ->
  #    ?SIZED(Size, gen_op(Size)).
  #
  #gen_op(_Size) ->
  #    gen_op2(int()).
  #
  #gen_op2(Gen) ->
  #    oneof([gen_updates(Gen), gen_update(Gen)]).
  #
  #gen_updates(Gen) ->
  #    {update, non_empty(list(gen_update(Gen)))}.
  #
  #gen_update(Gen) ->
  #    oneof([{add, Gen}, {remove, Gen},
  #           {add_all, non_empty(short_list(Gen))},
  #           {remove_all, non_empty(short_list(Gen))}]).
  #
  #short_list(Gen) ->
  #    ?SIZED(Size, resize(Size div 2, list(resize(Size, Gen)))).
  #
  #init_state() ->
  #    {0, dict:new()}.
  #
  #do_updates(_ID, [], _OldState, NewState) ->
  #    NewState;
  #do_updates(ID, [Update | Rest], OldState, NewState) ->
  #    case {Update, update_expected(ID, Update, NewState)} of
  #        {{Op, _Arg}, NewState} when Op == remove;
  #                                   Op == remove_all ->
  #            OldState;
  #        {_, NewNewState} ->
  #            do_updates(ID, Rest, OldState, NewNewState)
  #    end.
  #
  #update_expected(ID, {update, Updates}, State) ->
  #    do_updates(ID, Updates, State, State);
  #update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
  #    Cnt = Cnt0+1,
  #    ToAdd = {Elem, Cnt},
  #    {A, R} = dict:fetch(ID, Dict),
  #    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
  #update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
  #    {A, R} = dict:fetch(ID, Dict),
  #    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
  #    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
  #update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
  #    {FA, FR} = dict:fetch(ID, Dict),
  #    {TA, TR} = dict:fetch(SourceID, Dict),
  #    MA = sets:union(FA, TA),
  #    MR = sets:union(FR, TR),
  #    {Cnt, dict:store(ID, {MA, MR}, Dict)};
  #update_expected(ID, create, {Cnt, Dict}) ->
  #    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
  #update_expected(ID, {add_all, Elems}, State) ->
  #    lists:foldl(fun(Elem, S) ->
  #                       update_expected(ID, {add, Elem}, S) end,
  #               State,
  #               Elems);
  #update_expected(ID, {remove_all, Elems}, {_Cnt, Dict}=State) ->
  #    %% Only if _all_ elements are in the set do we remove any elems
  #    {A, R} = dict:fetch(ID, Dict),
  #    %% DO NOT consider tombstones as "in" the set, orswot does not have idempotent remove
  #    In = sets:subtract(A, R),
  #    Members = [ Elem || {Elem, _X} <- sets:to_list(In)],
  #    case is_sub_bag(Elems, lists:usort(Members)) of
  #        true ->
  #            lists:foldl(fun(Elem, S) ->
  #                                update_expected(ID, {remove, Elem}, S) end,
  #                        State,
  #                        Elems);
  #        false ->
  #            State
  #    end.
  #
  #eqc_state_value({_Cnt, Dict}) ->
  #    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
  #                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
  #                       {sets:new(), sets:new()},
  #                       Dict),
  #    Remaining = sets:subtract(A, R),
  #    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
  #    lists:usort(Values).
  #
  #%% Bag1 and Bag2 are multisets if Bag1 is a sub-multset of Bag2 return
  #%% true, else false
  #is_sub_bag(Bag1, Bag2) when length(Bag1) > length(Bag2) ->
  #    false;
  #is_sub_bag(Bag1, Bag2) ->
  #    SubBag = lists:sort(Bag1),
  #    SuperBag = lists:sort(Bag2),
  #    is_sub_bag2(SubBag, SuperBag).
  #
  #is_sub_bag2([], _SuperBag) ->
  #    true;
  #is_sub_bag2([Elem | Rest], SuperBag) ->
  #    case lists:delete(Elem, SuperBag) of
  #        SuperBag ->
  #            false;
  #        SuperBag2 ->
  #            is_sub_bag2(Rest, SuperBag2)
  #    end.
  #end
end

