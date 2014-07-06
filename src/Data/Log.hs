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
-- Portability :  portable
--
-- General 'Log' and 'State' typeclasses. Collectively, these implement a state machine, and the
-- Raft algorithm essential is one application of the
-- <https://www.cs.cornell.edu/fbs/publications/SMSurvey.pdf replicated state machine model>
-- for implementing a distributed system.
--
-----------------------------------------------------------------------------

module Data.Log (

    Index,
    Log(..),
    defaultCommitEntries,
    fetchLatestEntries,
    State(..)

) where

-- local imports

-- external imports

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
A 'Log' of type @l@ is a sequence of entries of type @e@ such that
entries can be appended to the log starting at a particular 'Index' (and potentially
overwrite entries previously appended at the same index), fetched
from a particular 'Index', or committed up to a certain 'Index'. Once committed,
it is undefined whether attempting to fetch entries with an 'Index' < 'lastCommitted'
will succeed or throw an error, as some log implementations may throw away some
committed entries.

Each entry in the 'Log' defines an action that transforms a supplied initial 'State'
into a new 'State'.  Commiting a 'Log', given some initial 'State', applies the action contained in
each entry in sequence (starting at a specified 'Index') to some state of type @s@,
producing a new 'State' after committing as many entries as possible.

Each log implementation may choose the monad @m@ in which they operate.  Consumers of logs
should always use logs in a functional style: that is, after 'appendEntries' or 'commitEntries',
if the log returned from those functions is not fed into later functions, then results
may be unexpected.  While the underlying log implementation may itself be pure, log
methods are wrapped in a monad to support those implementations that may not be--such
as a log whose entries are read from disk.

Implementing 'commitEntries' is optional, as a default implementation is supplied
that will invoke 'commitEntry' and 'applyEntry' as needed. Implementations that wish
to optimize commiting batches of entires may choose to override this method.
-}
class (Monad m,State s m e) => Log l m e s | l -> e,l -> s,l -> m where
    {-|
    'Index' of last committed entry in the 'Log'.
    -}
    lastCommitted :: l -> Index
    {-|
    'Index' of last appended entry (e.g., the end of the 'Log').
    -}
    lastAppended :: l -> Index
    {-|
    Append new log entries into the 'Log' after truncating the log
    to remove all entries whose 'Index' is greater than or equal
    to the specified 'Index', although no entries will be overwritten
    if they are already committed.
    -}
    appendEntries :: l -> Index -> [e] -> m l
    {-|
    Retrieve a number of entries starting at the specified 'Index'
    -}
    fetchEntries :: l -> Index -> Int -> m [e]
    {-|
    For each uncommitted entry whose 'Index' is less than or equal to the
    specified index, apply the entry to the supplied 'State' using 'applyEntry',
    then mark the entry as committed in the 'Log'. Note that implementers are
    free to commit no entries, some entries, or all entries as needed to handle
    errors.  However, the implementation should eventually commit all entries
    if the calling application is well-behaved.
    -}
    commitEntries :: l -> Index -> s -> m (l,s)
    commitEntries = defaultCommitEntries

    {-|
    Records a single entry in the log as committed; note that
    this does not involve any external 'State' to which the entry
    must be applied, as that is a separate operation.
    -}
    commitEntry :: l -> Index -> e -> m l

    {-|
    Snapshot the current `Log` and `State` to persistant storage
    such that in the event of failure, both will be recovered from
    this point.
    -}
    checkpoint :: l -> s -> m (l, s)

{-|
'Log's operate on 'State': that is, when committing, the log applies each
entry to the current 'State', and produces a new 'State'. Application of each
entry operates within a chosen 'Monad', so implementers are free to implement 
'State' as needed (e.g., use 'IO', 'STM', etc.).
-}
class (Monad m) => State s m e where
    canApplyEntry :: s -> e -> m Bool
    applyEntry :: s -> e -> m s

{-|
An 'Index' is a logical offset into a 'Log'.

While the exact interpretation of the 'Index' is up to the 'Log'
implementation, by convention the first 'Index' in a log is  @0@.
-}
type Index = Int

{-|
Default implementation of `commitEntries`, which fetches all uncommitted entries
with `fetchEntries`, then commits them one by one with `commitEntry` until either the
result of `canApplyEntry` is false, or all entries are committed.
-}
defaultCommitEntries :: (Monad m,State s m e, Log l m e s) => l -> Index -> s -> m (l,s)
defaultCommitEntries initialLog index initialState = do
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
            can <- canApplyEntry oldState entry
            if can
                then do
                    newLog <- commitEntry oldLog commitIndex entry
                    newState <- applyEntry oldState entry
                    commit newLog newState (commitIndex + 1)  rest
                else return (oldLog,oldState)

{-|
Return all entries from the 'Log''s 'lastCommitted' time up to and
including the 'lastAppended' time.
-}
fetchLatestEntries :: (Monad m, Log l m e s) => l -> m (Index,[e])
fetchLatestEntries log = do
    let commitTime = lastCommitted log
        startTime = commitTime + 1
        count = lastAppended log - lastCommitted log
    entries <- fetchEntries log startTime count
    return (commitTime,entries)
