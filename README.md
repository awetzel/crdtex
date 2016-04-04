# Crdtex

Dot style CRDTs implementation : distributed nested data types in Elixir.
Elixir fork/rewrite of `riak_dt` using : 

- Elixir protocols to take any compatible CRDT as field value in a map 
- Erlang maps `%{}` instead of `dict`, `orddict`, `ordset`

The goal is to have a nicer API on `riak_dt` types than the erlang one
making profit from Erlang Map and Elixir features.

## Current State ##

All CRDTs from `riak_dt` compatible with `riak_dt_map` are ported into this
library, with there tests. Currently they all pass.

**BUT DO NOT USE IT IN PRODUCTION YET**

Since the goal is to have a nice API, the API is not stabilized yet so it will change soon.

## Usage



## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add crdtex to your list of dependencies in `mix.exs`:

        def deps do
          [{:crdtex, "~> 0.0.1"}]
        end

  2. Ensure crdtex is started before your application:

        def application do
          [applications: [:crdtex]]
        end

