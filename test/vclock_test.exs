defmodule VclockTest do
  use ExUnit.Case
  alias Crdtex.Vclock

  test "trivial tests" do
    a = Vclock.fresh ; b = Vclock.fresh

    a1 = Vclock.increment(:a, a)
    b1 = Vclock.increment(:b, b)

    assert Vclock.descends(a1,a)
    assert Vclock.descends(b1,b)
    refute Vclock.descends(a1,b1)

    a2 = Vclock.increment(:a, a1)
    c1 = Vclock.increment(:c, Vclock.merge([a2, b1]))

    assert Vclock.descends(c1, a2)
    assert Vclock.descends(c1, b1)
    refute Vclock.descends(b1, c1)
    refute Vclock.descends(b1, a1)
  end

  test "vclock accessors" do
    vc = [{"1",1},{"2",2}]
    assert 1 == Vclock.get_counter("1",vc)
    assert 2 == Vclock.get_counter("2",vc)
    assert 0 == Vclock.get_counter("3",vc)
    assert ["1","2"] == Vclock.all_nodes(vc)
  end

  test "vclock merge" do
    vc1 = [{"1",1},{"2",2},{"4",4}]
    vc2 = [{"3",3},{"4",3}]
    assert [] == Vclock.merge(Vclock.fresh)
    assert [{"1",1},{"2",2},{"3",3},{"4",4}] == Vclock.merge([vc1,vc2])
  end

  test "vclock merge less left" do
    vc1 = [{"5",5}]
    vc2 = [{"6",6},{"7",7}]
    assert [{"5",5},{"6",6},{"7",7}] == Vclock.merge([vc1,vc2])
  end

  test "vclock merge less right" do
    vc1 = [{"6", 6},{"7",7}]
    vc2 = [{"5", 5}]
    assert [{"5",5},{"6",6},{"7",7}] == Vclock.merge([vc1,vc2])
  end

  test "vclock merge same id" do
    vc1 = [{"1",1},{"2",1}]
    vc2 = [{"1",1},{"3",1}]
    assert [{"1",1},{"2",1},{"3",1}] == Vclock.merge([vc1,vc2])
  end
end
