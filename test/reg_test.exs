defmodule Crdtex.RegTest do
  use ExUnit.Case
  alias Crdtex.Reg

  test "new" do
    assert %Reg{val: nil, ts: 0} == Reg.new
  end

  test "value" do
    assert "the rain in spain" == 
      Crdtex.value(%Reg{val: "the rain in spain", ts: 19090})
    assert nil == Crdtex.value(Reg.new)
  end

  test "update assign" do
    reg = Reg.new
    {:ok, reg} = Crdtex.update(reg,:actor1,{:assign, :value1, 2})
    {:ok, reg} = Crdtex.update(reg,:actor1,{:assign, :value0, 1})
    assert %Reg{val: :value1, ts: 2} == reg
    {:ok, reg} = Crdtex.update(reg,:actor1,{:assign, :value2, 3})
    assert %Reg{val: :value2, ts: 3} == reg
  end

  test "update assign ts" do
    reg = Reg.new
    {:ok, reg} = Crdtex.update(reg,:actr,{:assign, :value0})
    {:ok, reg} = Crdtex.update(reg,:actr,{:assign, :value1})
    assert :value1 == reg.val
  end

  test "merge" do
    reg1 = %Reg{val: :old_value, ts: 3}
    reg2 = %Reg{val: :new_value, ts: 4}
    assert %Reg{val: nil, ts: 0} == Crdtex.merge(Reg.new,Reg.new)
    assert %Reg{val: :new_value, ts: 4} == Crdtex.merge(reg1,reg2)
    assert %Reg{val: :new_value, ts: 4} == Crdtex.merge(reg2,reg1)
  end

  test "equal" do
    reg1 = %Reg{val: :value1, ts: 1000}
    reg2 = %Reg{val: :value1, ts: 1000}
    reg3 = %Reg{val: :value1, ts: 1001}
    reg4 = %Reg{val: :value2, ts: 1000}
    refute Crdtex.equal(reg1,reg3)
    assert Crdtex.equal(reg1,reg2)
    refute Crdtex.equal(reg4,reg1)
  end

  test "query" do
    reg = Reg.new
    {:ok, reg} = Crdtex.update(reg,:a1,{:assign, :value, 100})
    assert 100 == Crdtex.value(reg, :timestamp)
  end

  test "stat" do
    reg0 = Reg.new
    {:ok, reg1} = Crdtex.update(reg0,1,{:assign, "abcd"})
    assert [value_size: 7] == Crdtex.stats(reg0)
    assert [value_size: 15] == Crdtex.stats(reg1)
    assert 15 == Crdtex.stat(reg1, :value_size)
    assert nil == Crdtex.stat(reg1, :actor_count)
  end
end
