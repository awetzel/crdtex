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
  @callback value(term, crdt) :: value
  @callback update(operation, actor, crdt) :: {:ok, crdt} | {:error, error}
  @callback update(operation, actor, crdt, context) :: {:ok, crdt} | {:error, error}

  @doc """
  When nested in a Map, some CRDTs need the logical clock of the
  top level Map to make context operations. This callback provides
  the clock and the crdt, and if relevant, returns to crdt with the
  given clock as it's own
  """
  @callback parent_clock(Crdtex.Vclock.t, crdt) :: crdt
  @callback merge(crdt, crdt) :: crdt
  @callback equal(crdt, crdt) :: boolean
  @callback to_binary(crdt) :: binary
  @callback to_binary(target_vers :: pos_integer, crdt) :: {:ok, binary} | {:error, :unsupported_version, vers :: pos_integer}
  @callback from_binary(binary) :: {:ok, crdt} | {:error,:invalid_binary} | {:error, :unsupported_version, vers :: pos_integer}
  @callback stats(crdt) :: [{atom, number}]
  @callback stat(atom, crdt) :: number | nil
  @callback to_version(pos_integer, crdt) :: crdt
end
