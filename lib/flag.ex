defmodule Crdtex.Flag do
  @moduledoc """
  riak_dt_od_flag: a flag that can be enabled and disabled as many
  times as you want, enabling wins, starts disabled.
  """
  @behaviour Crdtex.Behaviour
  alias Crdtex.Vclock
  defstruct vclock: [], dots: [], deferred: %{}
  @type t :: %Crdtex.Flag{vclock: Vclock.t, dots: [Crdtex.dot], deferred: deferred}

  @type od_flag_op :: :enable | :disable
  @type deferred :: MapSet.t

  @spec new() :: t
  def new, do: %__MODULE__{vclock: Vclock.fresh, dots: [], deferred: MapSet.new}

  defimpl Crdtex, for: Crdtex.Flag do
    alias Crdtex.Flag

    @doc "sets the clock in the flag to that `Clock'. Used by a containing Map for sub-CRDTs"
    @spec parent_clock(Flag.t,Vclock.t) :: Flag.t
    def parent_clock(flag,clock), do: %{flag| vclock: clock}
    
    @spec value(Flag.t) :: boolean
    def value(%{dots: []}), do: false
    def value(%{}), do: true
    
    @spec value(Flag.t,term) :: boolean
    def value(flag,_), do: value(flag)
    
    @spec update(Flag.t,Crdtex.actor|Crdtex.dot,Flag.od_flag_op) :: {:ok, Flag.t}
    def update(flag,dot,:enable) when is_tuple(dot) do
      {:ok,%{flag|vclock: Vclock.merge([[dot], flag.vclock]),
                  dots: Vclock.merge([[dot], flag.dots])}}
    end
    def update(flag,actor,:enable) do
      new_clock = Vclock.increment(flag.vclock,actor)
      dot = [{actor, Vclock.get_counter(new_clock,actor)}]
      {:ok, %{flag|vclock: new_clock, 
                   dots: Vclock.merge([dot, flag.dots])}}
    end
    def update(flag,_actor,:disable), do:
      disable(flag, nil)
    
    @spec disable(Flag.t, Crdtex.context) :: {:ok,Flag.t}
    def disable(flag, nil), do:
      {:ok, %{flag| dots: []}}
    def disable(flag, ctx) do
      new_dots = Vclock.subtract_dots(flag.dots, ctx)
      new_deferred = defer_disable(flag.deferred,flag.vclock, ctx)
      {:ok, %{flag| dots: new_dots, deferred: new_deferred}}
    end
    
    @doc """
    `update/4' is similar to `update/3' except that it takes a
    `context' (obtained from calling `precondition_context/1'). This
    context ensure that a `disable' operation does not `disable' a flag
    that is has not been `seen` enabled by the caller. For example, the
    flag has been enabled by `a', `b', and `c'. The user has a copy of
    the flag obtained by reading `a'. Subsequently all replicas merge,
    and the user sends a `disable' operation. The have only seen the
    enable of `a', yet they disable the concurrecnt `b' and `c'
    operations, unless they send a context obtained from `a'.
    """
    @spec update(Flag.t,Crdtex.context,Crdtex.actor | Crdtex.dot,Flag.od_flag_op) :: {:ok, Flag.t}
    def update(flag,nil,actor,op), do: update(flag,actor,op)
    def update(flag,_ctx,actor,:enable), do: update(flag,actor,:enable)
    def update(flag,ctx,_actor,:disable), do: disable(flag, ctx)
    
    # Determine if a `disable' operation needs to be deferred, or if it is complete.
    @spec defer_disable(Flag.deferred,Vclock.t,Vclock.t) :: Flag.deferred
    def defer_disable(deferred,clock, ctx) do
      if Vclock.descends(clock, ctx) do
        deferred
      else
        MapSet.put(deferred,ctx)
      end
    end
    
    @spec merge(Flag.t, Flag.t) :: Flag.t
    def merge(flag, flag), do: flag

    def merge(flag1,flag2) do
      new_clock = Vclock.merge([flag1.vclock, flag2.vclock])
      # drop all the LHS dots that are dominated by the rhs clock
      # drop all the RHS dots that dominated by the LHS clock
      # keep all the dots that are in both
      # save value as value of flag
      dots1 = MapSet.new(flag1.dots); dots2 = MapSet.new(flag2.dots)
      common = MapSet.intersection(dots1,dots2)
      unique1 = MapSet.difference(dots1,common) |> Enum.to_list
      unique2 = MapSet.difference(dots2,common) |> Enum.to_list
      keep1 = Vclock.subtract_dots(unique1, flag2.vclock)
      keep2 = Vclock.subtract_dots(unique2, flag1.vclock)
      new_dots = Vclock.merge([Enum.to_list(common), keep1, keep2])
      deferred = MapSet.union(flag1.deferred,flag2.deferred)
    
      apply_deferred(new_clock, new_dots, deferred)
    end
    
    defp apply_deferred(clock, dots, deferred) do
      acc0 = %Flag{vclock: clock, dots: dots, deferred: MapSet.new}
      Enum.reduce(deferred,acc0,fn ctx, acc->
        {:ok,acc} = disable(acc,ctx); acc
      end)
    end
    
    @spec equal(Flag.t, Flag.t) :: boolean
    def equal(flag1,flag2) do
      Vclock.equal(flag1.vclock,flag2.vclock) and
      Vclock.equal(flag1.dots,flag2.dots) and
      flag1.deferred == flag2.deferred
    end
    
    @spec stats(Flag.t) :: [{atom, integer}]
    def stats(flag) do
      [actor_count: stat(flag,:actor_count),
       dot_length: stat(flag,:dot_length),
       deferred_length: stat(flag,:deferred_length)]
    end
    
    @spec stat(Flag.t,atom) :: number | nil
    def stat(flag,:actor_count), do: length(flag.vclock)
    def stat(flag,:dot_length), do: length(flag.dots)
    def stat(flag,:deferred_length), do: MapSet.size(flag.deferred)
    def stat(_, _), do: nil

    @doc """
    the `precondition_context' is an opaque piece of state that
    can be used for context operations to ensure only observed state is
    affected.
    """
    @spec precondition_context(Flag.t) :: Crdtex.context
    def precondition_context(flag), do: flag.vclock
  end
end

