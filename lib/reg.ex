defmodule Crdtex.Reg do
  @moduledoc """
  An LWW Register CRDT.
  
  ## References

  - Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) [A comprehensive study of
    Convergent and Commutative Replicated Data Types](http://hal.upmc.fr/inria-00555588/)
  """
  @behaviour Crdtex.Behaviour

  defstruct val: nil, ts: 0
  @type t :: %Crdtex.Reg{val: term, ts: non_neg_integer}

  @type lwwreg_op :: {:assign, term, non_neg_integer} | {:assign, term}
  @type lww_q :: :timestamp

  @doc "Create a new, empty `lwwreg()'"
  @spec new :: t
  def new, do: %__MODULE__{}

  alias Crdtex.Reg

  defimpl Crdtex, for: Crdtex.Reg do
    @spec parent_clock(Reg.t,Vclock.t) :: Reg.t
    def parent_clock(reg,_), do: reg
    
    @doc "The single total value of a `gcounter()'."
    @spec value(Reg.t) :: term
    def value(reg), do: reg.val
    
    @doc "query for this `lwwreg()'.  `timestamp' is the only query option."
    @spec value(Reg.t,Reg.lww_q) :: non_neg_integer
    def value(reg,:timestamp), do: reg.ts
    
    @doc "Assign a `Value' to the `lwwreg()' associating the update with time `TS'"
    @spec update(Reg.t,Crdtex.actor | Crdtex.dot,Reg.lwwreg_op()) :: {:ok, Reg.t}
    def update(%{ts: old_ts}=_reg,_actor,{:assign, val, ts}) when is_integer(ts) and ts > 0 and ts >= old_ts, do:
      {:ok, %Reg{val: val, ts: ts}}
    def update(old_reg,_actor,{:assign, _val, _ts}), do: {:ok, old_reg}

    # For when users don't provide timestamps
    # don't think it is a good idea to mix server and client timestamps
    def update(%{val: _old_val, ts: old_ts}=old_reg,_actor,{:assign, val}) do
      micro_epoch = :erlang.system_time(:micro_seconds)
      lww = if micro_epoch > old_ts, do: %Reg{val: val, ts: micro_epoch}, else: old_reg
      {:ok,lww}
    end
    
    def update(reg,_ctx,actor,op), do: update(reg,actor,op)
    
    @doc "Merge two `lwwreg()'s to a single `lwwreg()'. This is the Least Upper Bound function described in the literature."
    @spec merge(Reg.t, Reg.t) :: Reg.t
    def merge(%{ts: ts1}=reg1,%{ts: ts2}=_reg2) when ts1 > ts2, do: reg1
    def merge(%{ts: ts1}=_reg1,%{ts: ts2}=reg2) when ts2 > ts1, do: reg2
    def merge(%{val: val1}=reg1,%{val: val2}=_reg2) when val1 >= val2, do: reg1
    def merge(_reg1, reg2), do: reg2
    
    @doc """
    Are two `lwwreg()'s structurally equal? This is not `value/1' equality.
    Two registers might represent the value `armchair', and not be `equal/2'. Equality here is
    that both registers contain the same value and timestamp.
    """
    @spec equal(Reg.t, Reg.t) :: boolean
    def equal(reg, reg), do: true
    def equal(_, _), do: false
    
    @spec stats(Reg.t) :: [{atom, number}]
    def stats(reg), do: [value_size: stat(reg,:value_size)]
    
    @spec stat(Reg.t,atom) :: number | nil
    def stat(reg,:value_size), do: :erlang.external_size(reg.val)
    def stat(_, _), do: nil
  end
end
