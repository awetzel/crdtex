defmodule Crdtex.CounterTest do
  use ExUnit.Case
  alias Crdtex.Counter

  #use EQC.ExUnit
  #def counters do
  #  let {ops,actors} <- {non_empty(list(gen_op)),non_empty(list(bitstring(16*8)))} do
  #    Enum.reduce(ops,{Counter.new,1}, fn op, {counter,evt}->
  #      actor = Enum.random(actors)
  #      {:ok, counter} = Crdtex.update(counter,{actor,evt},op)
  #      {counter, evt+1}
  #    end) |> elem(0)
  #  end
  #end

  #def init_state, do: 0
  #def gen_op, do:
  #  oneof([:increment,{:increment, nat},:decrement,{:decrement, nat}])

  #def update_expected(_id,  :increment, prev), do: prev+1
  #def update_expected(_id,  :decrement, prev), do: prev-1
  #def update_expected(_id,  {:increment, by}, prev), do: prev+by
  #def update_expected(_id,  {:decrement, by}, prev), do: prev-by
  #def update_expected(_id,  _op, prev), do: prev

  #def eqc_state_value(s), do: s

  test "new" do
    assert Crdtex.value(Counter.new) == 0
  end

  def make_counter(evt, ops) do
    Enum.reduce(ops,{Counter.new,evt}, fn {actor, op}, {counter,evt}->
     evt = evt + 1
     {:ok, counter} = Crdtex.update(counter, {actor,evt}, op)
     {counter,evt}
    end)
  end

  def make_counter(ops), do: elem(make_counter(0, ops),0)

  test "value after ops" do
    assert make_counter(a: :increment, b: {:increment, 13},
                        b: {:decrement, 10}, c: :increment,
                        d: :decrement) |> Crdtex.value == 4
    assert make_counter([]) |> Crdtex.value == 0
    assert make_counter(a: {:increment,3}, a: {:decrement, 3},
                        b: :decrement, b: :increment,
                        c: :increment, c: :decrement) |> Crdtex.value == 0
  end

  test "update increment" do
    cnt = Counter.new
    {:ok, cnt} = Crdtex.update(cnt, {:a, 1}, :increment)
    {:ok, cnt} = Crdtex.update(cnt, {:b, 1}, :increment)
    {:ok, cnt} = Crdtex.update(cnt, {:a, 2}, :increment)
    assert Crdtex.value(cnt) == 3
  end

  test "update increment by" do
    cnt = Counter.new
    {:ok, cnt} = Crdtex.update(cnt, {:a, 1}, {:increment,7})
    assert Crdtex.value(cnt) == 7
  end

  test "update decrement" do
    cnt = Counter.new
    {:ok, cnt} = Crdtex.update(cnt, {:a, 1}, :increment)
    {:ok, cnt} = Crdtex.update(cnt, {:b, 1}, :increment)
    {:ok, cnt} = Crdtex.update(cnt, {:a, 2}, :increment)
    {:ok, cnt} = Crdtex.update(cnt, {:a, 3}, :decrement)
    assert Crdtex.value(cnt) == 2
  end

  test "update decrement by" do
    cnt = Counter.new
    {:ok, cnt} = Crdtex.update(cnt, {:a, 1}, {:increment,7})
    {:ok, cnt} = Crdtex.update(cnt, {:a, 2}, {:decrement,5})
    assert Crdtex.value(cnt) == 2
  end

  test "update neg increment by" do
    cnt = Counter.new
    {:ok, cnt} = Crdtex.update(cnt, {:a, 1}, {:increment,-8})
    {:ok, cnt} = Crdtex.update(cnt, {:a, 2}, {:decrement,-7})
    assert Crdtex.value(cnt) == -1
  end

  test "merge" do
    assert Crdtex.merge(Counter.new,Counter.new) == Counter.new

    {cnt1,evt} = make_counter(0, a: :increment, b: {:increment,2}, d: {:increment,1})
    {cnt2,_evt}= make_counter(evt, c: {:increment,3}, d: {:increment,3})
    assert Crdtex.value(Crdtex.merge(cnt1,cnt2)) == 9
  end

  test "counter equal" do
    cnt1 = make_counter(a: {:increment,2}, a: :decrement,
                        b: :increment, c: :decrement, d: :increment)
    cnt2 = make_counter(a: :increment, b: {:increment, 4}, c: :increment)
    cnt3 = make_counter(a: {:increment, 2}, a: :decrement,
                        b: :increment, c: :decrement, d: :increment)
    refute Crdtex.equal(cnt1, cnt2)
    assert Crdtex.equal(cnt1, cnt3)
  end

  test "usage" do
    cnt1 = Counter.new
    cnt2 = Counter.new
    assert Crdtex.equal(cnt1,cnt2)

    {:ok, cnt1} = Crdtex.update(cnt1, {:a1,1}, {:increment, 2})
    {:ok, cnt2} = Crdtex.update(cnt2, {:a2,1}, :increment)
    cnt3 = Crdtex.merge(cnt1,cnt2)
    {:ok, cnt2} = Crdtex.update(cnt2, {:a3,1}, {:increment, 3})
    {:ok, cnt3} = Crdtex.update(cnt3, {:a4,1}, :increment)
    {:ok, cnt3} = Crdtex.update(cnt3, {:a1,2}, :increment)
    {:ok, cnt3} = Crdtex.update(cnt3, {:a5,1}, {:decrement,2})
    {:ok, cnt2} = Crdtex.update(cnt2, {:a2,2}, :decrement)

    assert %Counter{
             vclock: [a1: 2, a2: 2, a3: 1, a4: 1, a5: 1],
             entries: %{a1: {2,3,0}, a2: {2,1,1}, 
                        a3: {1,3,0}, a4: {1,1,0}, a5: {1,0,2}}
           } == Crdtex.merge(cnt3,cnt2)
  end

  test "stat" do
    cnt = cnt0 = Counter.new
    {:ok, cnt} = Crdtex.update(cnt,{:a1,1},{:increment, 50})
    {:ok, cnt} = Crdtex.update(cnt,{:a2,1},{:increment, 50})
    {:ok, cnt} = Crdtex.update(cnt,{:a3,1},{:decrement, 15})
    {:ok, cnt} = Crdtex.update(cnt,{:a4,1},{:decrement, 10})

    assert Crdtex.stats(cnt0) == [actor_count: 0]
    assert Crdtex.stat(cnt,:actor_count) == 4
    assert Crdtex.stat(cnt,:max_dot_length) == nil
  end
end
