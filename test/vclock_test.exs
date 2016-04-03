defmodule VclockTest do
  use ExUnit.Case
  import Crdtex.Vclock
  doctest Crdtex.Vclock

  test "trivial tests" do
    a = fresh ; b = fresh

    a1 = increment(a, :a)
    b1 = increment(b, :b)

    assert descends(a1,a)
    assert descends(b1,b)
    refute descends(a1,b1)

    a2 = increment(a1, :a)
    c1 = increment(merge([a2, b1]), :c)

    assert descends(c1, a2)
    assert descends(c1, b1)
    refute descends(b1, c1)
    refute descends(b1, a1)
  end

  test "vclock accessors" do
    vc = [{"1",1},{"2",2}]
    assert 1 == get_counter(vc,"1")
    assert 2 == get_counter(vc,"2")
    assert 0 == get_counter(vc,"3")
    assert ["1","2"] == all_nodes(vc)
  end

  test "vclock merge" do
    vc1 = [{"1",1},{"2",2},{"4",4}]
    vc2 = [{"3",3},{"4",3}]
    assert [] == merge(fresh)
    assert [{"1",1},{"2",2},{"3",3},{"4",4}] == merge([vc1,vc2])
  end

  test "vclock merge less left" do
    vc1 = [{"5",5}]
    vc2 = [{"6",6},{"7",7}]
    assert [{"5",5},{"6",6},{"7",7}] == merge([vc1,vc2])
  end

  test "vclock merge less right" do
    vc1 = [{"6", 6},{"7",7}]
    vc2 = [{"5", 5}]
    assert [{"5",5},{"6",6},{"7",7}] == merge([vc1,vc2])
  end

  test "vclock merge same id" do
    vc1 = [{"1",1},{"2",1}]
    vc2 = [{"1",1},{"3",1}]
    assert [{"1",1},{"2",1},{"3",1}] == merge([vc1,vc2])
  end
end
