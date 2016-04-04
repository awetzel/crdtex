defmodule Crdtex.FlagTest do
  use ExUnit.Case
  alias Crdtex.Flag

  test "disable" do
    {:ok, a} = Crdtex.update(Flag.new,:a,:enable)
    {:ok, b} = Crdtex.update(Flag.new,:b,:enable)
    c = a
    {:ok, a} = Crdtex.update(a,:a,:disable)
    a = Crdtex.merge(a,b)
    {:ok, b} = Crdtex.update(b,:b,:disable)
    merged = c |> Crdtex.merge(a) |> Crdtex.merge(b)
    refute Crdtex.value(merged)
  end

  test "new" do
    refute Crdtex.value(Flag.new)
  end

  test "update enable" do
    f = Flag.new
    {:ok, f} = Crdtex.update(f,1,:enable)
    assert Crdtex.value(f)
  end

  test "update enable multi" do
    f = Flag.new
    {:ok, f} = Crdtex.update(f, 1, :enable)
    {:ok, f} = Crdtex.update(f, 1, :disable)
    {:ok, f} = Crdtex.update(f, 1, :enable)
    assert Crdtex.value(f)
  end

  test "merge offs" do
    f = Flag.new
    refute Crdtex.value(Crdtex.merge(f,f))
  end

  test "merge simple" do
    f0 = Flag.new
    {:ok, f1} = Crdtex.update(f0,1,:enable)
    assert Crdtex.value(Crdtex.merge(f1,f0))
    assert Crdtex.value(Crdtex.merge(f0,f1))
    assert Crdtex.value(Crdtex.merge(f1,f1))
  end

  test "merge concurrent" do
    f0 = Flag.new
    {:ok, f1} = Crdtex.update(f0,1,:enable)
    {:ok, f2} = Crdtex.update(f1,1,:disable)
    {:ok, f3} = Crdtex.update(f1,1,:enable)
    assert Crdtex.value(Crdtex.merge(f1,f3))
    refute Crdtex.value(Crdtex.merge(f1,f2))
    assert Crdtex.value(Crdtex.merge(f2,f3))
  end

  test "stat" do
    {:ok, f} = Crdtex.update(Flag.new,1,:enable)
    {:ok, f} = Crdtex.update(f, 1, :enable)
    {:ok, f} = Crdtex.update(f, 2, :enable)
    {:ok, f} = Crdtex.update(f, 3, :enable)
    assert [actor_count: 3, dot_length: 3, deferred_length: 0] == Crdtex.stats(f)
    {:ok, f} = Crdtex.update(f,4,:disable) # Observed-disable doesn't add an actor
    assert [actor_count: 3, dot_length: 0, deferred_length: 0] == Crdtex.stats(f)
    assert 3 == Crdtex.stat(f,:actor_count)
    assert nil == Crdtex.stat(f,:max_dot_length)
  end
end
