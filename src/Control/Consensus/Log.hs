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
    LogIO,
    LogTime(..),
    Server(..)

) where

-- local imports

-- external imports

import Network.Endpoints

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
class (LogTime t) => Log l t m e s | l -> t,l -> e,l -> s,l -> m where
    {-|
    Create a new `Log`.
    -}
    mkLog :: m l
    {-|
    `LogTime` of last committed entry in the `Log`.
    -}
    lastCommitted :: l -> t
    {-|
    `LogTime` of last appended entry (e.g., the end of the `Log`).
    -}
    lastAppended :: l -> t
    {-|
    Append new log entries into the `Log` after truncating the log
    to remove all entries whose `LogTime` is greater than or equal
    to the specified `LogTime`, although no entries will be overwritten
    if they are already committed.
    -}
    appendEntries :: l -> t -> [e] -> m l
    {-|
    Retrieve a number of entries starting at the specified `LogTime`
    -}
    fetchEntries :: l -> t -> Int -> m [e]
    {-|
    Commit all entries in the log whose `LogTime` is less than or equal
    to the specified `LogTime`.
    -}
    commitEntries :: l -> t -> s -> m (l,s)

{-|
Return all entries from the `Log`'s `lastCommitted` time up to and
including the `lastAppended` time.
-}
fetchLatestEntries :: (LogTime t, Monad m, Log l t m e s) => l -> m (t,[e])
fetchLatestEntries log = do
    let commitTime = lastCommitted log
        startTime = nextLogTime commitTime
        count = (logIndex $ lastAppended log) - (logIndex $ lastCommitted log)
    entries <- fetchEntries log startTime count
    return (commitTime,entries)

{-|
Abstract class for defining an offset into a `Log`
-}
class (Ord t) => LogTime t where
    logIndex :: t -> Index
    nextLogTime :: t -> t

{-|
Variant of 'Log' useful for implementations that need to perform 'IO'.
-}
class (Log l t IO e s) => LogIO l t e s

data Server l t e v = (LogIO l t e v) => Server {
    serverName :: Name,
    serverLog :: l,
    serverState :: v
}
