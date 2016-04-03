defmodule Crdtex do
  @type crdt :: term
  @type operation :: term
  @type actor :: term
  @type value :: term
  @type error :: term
  @type dot :: {actor, pos_integer}
  @type context :: Crdtex.Vclock.t | nil

  @callback new :: crdt
  @callback value(crdt) :: term
  @callback value(crdt, term) :: value
  @callback update(crdt, actor, operation) :: {:ok, crdt} | {:error, error}
  @callback update(crdt, context, actor, operation) :: {:ok, crdt} | {:error, error}

  @doc """
  When nested in a Map, some CRDTs need the logical clock of the
  top level Map to make context operations. This callback provides
  the clock and the crdt, and if relevant, returns to crdt with the
  given clock as it's own
  """
  @callback parent_clock(crdt, Crdtex.Vclock.t) :: crdt
  @callback merge(crdt, crdt) :: crdt
  @callback equal(crdt, crdt) :: boolean
  @callback to_binary(crdt) :: binary
  @callback to_binary(crdt, target_vers :: pos_integer) :: {:ok, binary} | {:error, :unsupported_version, vers :: pos_integer}
  @callback from_binary(binary) :: {:ok, crdt} | {:error,:invalid_binary} | {:error, :unsupported_version, vers :: pos_integer}
  @callback stats(crdt) :: [{atom, number}]
  @callback stat(crdt, atom) :: number | nil
  @callback to_version(crdt, pos_integer) :: crdt
end
