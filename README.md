raft
=======

[![Build Status](https://travis-ci.org/hargettp/raft.svg?branch=master)](https://travis-ci.org/hargettp/raft)

Haskell implementation of Raft consensus algorithm.

Not 100% functional, but making progress.

Both 3-node and 5-node cluster tests are passing, including the "stability" tests which validate
that once a leader is chosen, the leadership doesn't change.

Simple single-action client test passes, so both RPC calls of the Raft protocol appear to function.
