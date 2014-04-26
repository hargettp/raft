{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Log
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- General 'Log' typeclass.
--
-----------------------------------------------------------------------------

module Control.Consensus.Log (

    Index,
    Log(..),
    fetchLatestEntries,
    State(..)

) where

-- local imports

-- external imports

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-
An index is a logical offset into a log.
-}
type Index = Int

{-
A log of type @l@ is a sequence of entries of type @e@ such that
entries can be appended to the log starting at a particular 'Index' (and potentially
overwrite entries previously appended at the same index), fetched
from a particular 'Index', or committed up to a certain 'Index'. Once committed,
it is undefined whether attempting to fetch entries with an 'Index' < 'lastCommitted'
will succeed or throw an error, as some log implementations may throw away some
committed entries.

Each entry in the log defines an action that transforms a supplied initial state
into a new state.  Commiting a log, given some initial state, applies the action contained in
each entry in sequence (starting at a specified 'Index') to some state of type @s@,
producing a new state after committing as many entries as possible.

Each log implementation may choose the monad @m@ in which they operate.  Consumers of logs
should always use logs in a functional style: that is, after 'appendEntries' or 'commitEntries',
if the log returned from those functions is not fed into later functions, then results
may be unexpected.  While the underyling log implementation may itself be pure, log
methods are wrapped in a monad to support those implementations that may not be--such
as a log whose entries are read from disk.
-}
class (Monad m,State s m e) => Log l m e s | l -> e,l -> s,l -> m where
    {-|
    Create a new `Log`.
    -}
    mkLog :: m l
    {-|
    `Index` of last committed entry in the `Log`.
    -}
    lastCommitted :: l -> Index
    {-|
    `Index` of last appended entry (e.g., the end of the `Log`).
    -}
    lastAppended :: l -> Index
    {-|
    Append new log entries into the `Log` after truncating the log
    to remove all entries whose `Index` is greater than or equal
    to the specified `Index`, although no entries will be overwritten
    if they are already committed.
    -}
    appendEntries :: l -> Index -> [e] -> m l
    {-|
    Retrieve a number of entries starting at the specified `Index`
    -}
    fetchEntries :: l -> Index -> Int -> m [e]
    {-|
    For each uncommitted entry whose `Index` is less than or equal to the
    specified index, apply the entry to the supplied `State` using `applyEntry`,
    then mark the entry as committed in the `Log`. Note that implementers are
    free to commit no entries, some entries, or all entries as needed to handle
    errors.  However, the implementation should eventually commit all entries
    if the calling application is well-behaved.
    -}
    commitEntries :: l -> Index -> s -> m (l,s)
    commitEntries initialLog index initialState = do
        let committed = lastCommitted initialLog
            count = index - committed
        if count > 0
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetchEntries initialLog nextCommitted count
                commit initialLog initialState nextCommitted uncommitted 
            else return (initialLog,initialState)
        where
            commit oldLog oldState _ [] = do
                return (oldLog,oldState)
            commit oldLog oldState commitIndex (entry:rest) = do
                newLog <- commitEntry oldLog commitIndex entry
                newState <- applyEntry oldState entry
                commit newLog newState (commitIndex + 1)  rest

    {-|
    Records a single entry in the log as committed; note that
    this does not involve any external `State` to which the entry
    must be applied, as that is a separate operation.
    -}
    commitEntry :: l -> Index -> e -> m l

{-
`Log`s operate on `State`: that is, when committing, the log applies each
entry to the current `State`, and produces a new `State`. Application of each
entry operates within a chosen `Monad`, so implementers are free to implement 
`State` as needed (e.g., use `IO`, `STM`, etc.).
-}
class (Monad m) => State s m e where
    applyEntry :: s -> e -> m s

{-|
Return all entries from the `Log`'s `lastCommitted` time up to and
including the `lastAppended` time.
-}
fetchLatestEntries :: (Monad m, Log l m e s) => l -> m (Index,[e])
fetchLatestEntries log = do
    let commitTime = lastCommitted log
        startTime = commitTime + 1
        count = lastAppended log - lastCommitted log
    entries <- fetchEntries log startTime count
    return (commitTime,entries)
