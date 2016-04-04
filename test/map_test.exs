defmodule Crdtex.MapTest do
  use ExUnit.Case
  alias Crdtex.Map
  alias Crdtex.Set
  alias Crdtex.Flag
  alias Crdtex.Reg
  alias Crdtex.Counter

  test "field association" do
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :X, {:add, 0},fn-> Set.new end}]})
    {:ok, b} = Crdtex.update(Map.new,:b,{:update, [{:update, :X, {:add, 0},fn-> Set.new end}]})
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:update, :X, {:remove, 0},fn-> Set.new end}]})
    c = a
    {:ok, c} = Crdtex.update(c,:c,{:update, [{:remove, :X}]})
    assert Crdtex.merge(a,Crdtex.merge(b,c)) == Crdtex.merge(Crdtex.merge(a,b),c)
    assert Crdtex.value(Crdtex.merge(Crdtex.merge(a,c),b)) == Crdtex.value(Crdtex.merge(Crdtex.merge(a,b),c))
    assert Crdtex.merge(Crdtex.merge(a,c),b) == Crdtex.merge(Crdtex.merge(a,b),c)
  end

  test "clock" do
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :X, {:add, 0},fn-> Set.new end}]})
    b = a
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:update, :X, {:add, 1}}]})
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:update, :X, {:remove, 0}}]})
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:remove, :X}]})
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:update, :X, {:add, 2}, fn-> Set.new end}]})
    ab = Crdtex.merge(a, b)
    assert [{:X,[1,2]}] == Crdtex.value(ab)
  end

  test "remfield" do
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :X, {:add, 0},fn-> Set.new end}]})
    b = a
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:update, :X, {:remove, 0}}]})
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:remove, :X}]})
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:update, :X, {:add, 2},fn-> Set.new end}]})
    ab = Crdtex.merge(a,b)
    assert [{:X,[2]}] == Crdtex.value(ab)
  end

  test "present but removed" do
    # Bug found by EQC, not dropping dots in Crdtex.merge when an element is
    # present in both Maos leads to removed items remaining after Crdtex.merge.
    # Add Z to A
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :Z, {:assign,"A"},fn-> Reg.new end}]})
    # Replicate it to C so A has 'Z'->{a, 1}
    c = a
    # Remove Z from A
    {:ok, a} = Crdtex.update(a,:a,{:update, remove: :Z})
    #  Add Z to B, a new replica
    {:ok, b} = Crdtex.update(Map.new,:b,{:update, [{:update, :Z, {:assign, "B"},fn-> Reg.new end}]})
    # Replicate B to A, so now A has a Z, the one with a Dot of
    # {b,1} and clock of [{a, 1}, {b, 1}]
    a = Crdtex.merge(b, a)
    # Remove the 'Z' from B replica
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:remove, :Z}]})
    #  Both C and A have a 'Z', but when they Crdtex.merge, there should be
    #  no 'Z' as C's has been removed by A and A's has been removed by C.
    merged = Enum.reduce([c,b],a,&Crdtex.merge(&1,&2))
    assert [] == Crdtex.value(merged)
  end

  test "no dots left" do
    # A bug EQC found where dropping the dots in Crdtex.merge was not enough if
    # you then store the Crdtex.value with an empty clock (derp).
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :Z, {:assign, "A"},fn-> Reg.new end}]})
    {:ok, b} = Crdtex.update(Map.new,:b,{:update, [{:update, :Z, {:assign, "B"},fn-> Reg.new end}]})
    c = a ## replicate A to empty C
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:remove, :Z}]})
    # replicate B to A, now A has B's 'Z'
    a = Crdtex.merge(a, b)
    # Remove B's 'Z'
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:remove, :Z}]})
    # Replicate C to B, now B has A's old 'Z'
    b = Crdtex.merge(b, c)
    # Merge everytyhing, without the fix You end up with 'Z' present, with no dots
    merged = Enum.reduce([b,c],a,&Crdtex.merge(&1,&2))
    assert [] == Crdtex.value(merged)
  end

  test "tombstone remove" do
    # A reset-remove bug eqc found where dropping a superseded dot lost
    # field remove Crdtex.merge information the dropped dot contained, adding
    # the tombstone fixed this.
    a=b=Map.new
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:update, :X, {:add, 0},fn-> Set.new end}]})
    # Replicate!
    b = Crdtex.merge(a, b)
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:remove, :X}]})
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:update, :X, {:add, 1},fn-> Set.new end}]})
    # Replicate
    a = Crdtex.merge(a, b)
    # that remove of F from A means remove the 0 A added to F
    assert [{:X, [1]}] == Crdtex.value(a)
    {:ok, b} = Crdtex.update(b,:b,{:update, [{:update, :X, {:add, 2}}]})
    # replicate to A
    a = Crdtex.merge(a, b)
    # final Crdtex.values
    final = Crdtex.merge(a, b)
    # before adding the tombstone, the dropped dots were simply
    # Crdtex.merged with the surviving field. When the second update to B
    # was Crdtex.merged with A, that information contained in the superseded
    # field in A at {b,1} was lost (since it was Crdtex.merged into the
    # _VALUE_). This casued the [0] from A's first dot to
    # resurface. By adding the tombstone, the superseded field Crdtex.merges
    # it's tombstone with the surviving {b, 2} field so the remove
    # information is preserved, even though the {b, 1} Crdtex.value is
    # dropped. Pro-tip, don't alter the CRDTs' Crdtex.values in the Crdtex.merge!
    assert [{:X,[1,2]}] == Crdtex.value(final)
  end

  # This test is a regression test for a counter example found by eqc.
  # The previous version of riak_dt_map used the `dot' from the field
  # update/creation event as key in `Crdtex.merge_left/3'. Of course multiple
  # fields can be added/updated at the same time. This means they get
  # the same `dot'. When merging two replicas, it is possible that one
  # has removed one or more of the fields added at a particular `dot',
  # which meant a function clause error in `Crdtex.merge_left/3'. The
  # structure was wrong, it didn't take into account the possibility
  # that multiple fields could have the same `dot', when clearly, they
  # can. This test fails with `dot' as the key for a field in
  # `Crdtex.merge_left/3', but passes with the current structure, of
  # `{field(), dot()}' as key.
  test "dot key" do
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :X, {:add, "a"}, fn->Set.new end}, {:update, :Y, :enable,fn->Flag.new end}]})
    b = a
    {:ok, a} = Crdtex.update(a,:a,{:update, [{:remove, :Y}]})
    assert [{:X,["a"]}] == Crdtex.value(Crdtex.merge(b,a))
  end

  test "stat" do
    map0 = Map.new
    {:ok, map1} = Crdtex.update(map0,:a1,{:update, [
          {:update, :c, :increment, fn-> Counter.new end},
          {:update, :s, {:add, "A"},fn-> Set.new end},
          {:update, :m, {:update, [{:update, :ss, {:add, 0},fn-> Set.new end}]}, fn-> Map.new end},
          {:update, :l, {:assign, "a", 1}, fn-> Reg.new end},
          {:update, :l2,{:assign, "b", 2}, fn-> Reg.new end}
        ]})
    {:ok, map2} = Crdtex.update(map1,:a2,{:update, [{:update, :l, {:assign, "foo", 3}}]})
    {:ok, map3} = Crdtex.update(map1,:a3,{:update, [{:update, :l, {:assign, "bar", 4}}]})
    map4 = Crdtex.merge(map2, map3)
    assert [actor_count: 0, field_count: 0, duplication: 0, deferred_length: 0] == Crdtex.stats(map0)
    assert 3 == Crdtex.stat(map4,:actor_count)
    assert 5 == Crdtex.stat(map4,:field_count)
    assert nil == Crdtex.stat(map4,:waste_pct)
    assert 1 == Crdtex.stat(map4,:duplication)

    {:ok, map5} = Crdtex.update(map4,:a3,{:update, [{:update, :l3, {:assign, "baz", 5}, fn-> Reg.new end}]})
    assert 6 == Crdtex.stat(map5,:field_count)
    assert 1 == Crdtex.stat(map5,:duplication)
    # Updating field {l, riak_dt_lwwreg} Crdtex.merges the duplicates to a single field
    {:ok, map6} = Crdtex.update(map5,:a2,{:update, [{:update, :l, {:assign, "bim", 6}}]})
    assert 0 == Crdtex.stat(map6,:duplication)
    {:ok, map7} = Crdtex.update(map6,:a1,{:update, [{:remove, :l}]})
    assert 5 == Crdtex.stat(map7,:field_count)
  end

  test "equal" do
    {:ok, a} = Crdtex.update(Map.new,:a,{:update, [{:update, :W, {:add, "a"},fn-> Set.new end}, 
                                                   {:update, :X, :enable,fn-> Flag.new end}]})
    {:ok, b} = Crdtex.update(Map.new,:b,{:update, [{:update, :Y, {:add, "a"},fn-> Set.new end}, 
                                                   {:update, :Z, :enable,fn-> Flag.new end}]})
    refute Crdtex.equal(a,b)
    c = Crdtex.merge(a, b)
    d = Crdtex.merge(b, a)
    assert Crdtex.equal(c,d)
    assert Crdtex.equal(a,a)
  end
end
