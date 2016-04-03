defprotocol Crdtex do
  @type crdt :: Crdtex.t
  @type operation :: term
  @type actor :: term
  @type value :: term
  @type error :: term
  @type dot :: {actor, pos_integer}
  @type context :: Crdtex.Vclock.t | nil

  @spec value(crdt) :: term
  def value(crdt)

  @spec value(crdt :: t, term) :: value
  def value(crdt,term)

  @spec update(crdt, actor, operation) :: {:ok, crdt} | {:error, error}
  def update(crdt,actor,operation)

  @spec update(crdt, context, actor, operation) :: {:ok, crdt} | {:error, error}
  def update(crdt, context, actor, operation)

  @doc """
  When nested in a Map, some CRDTs need the logical clock of the
  top level Map to make context operations. This callback provides
  the clock and the crdt, and if relevant, returns to crdt with the
  given clock as it's own
  """
  @spec parent_clock(crdt, Crdtex.Vclock.t) :: crdt
  def parent_clock(crdt,vclock)

  @spec merge(crdt, crdt) :: crdt
  def merge(crdt,crdt)

  @spec equal(crdt, crdt) :: boolean
  def equal(crdt,crdt)

  @spec stats(crdt) :: [{atom, number}]
  def stats(crdt)

  @spec stat(crdt, atom) :: number | nil
  def stat(crdt, atom)
end

defmodule Crdtex.Behaviour do
  @callback new :: Crdtex.t
end
