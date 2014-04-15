{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts #-}

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
    LogIO,
    LogTime(..)

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
class Log l m e s | l -> e,l -> s, l -> m where
    mkLog :: m l
    lastCommitted :: l -> Index
    lastAppended :: l -> Index
    appendEntries :: l -> Index -> [e] -> m l
    fetchEntries :: l -> Index -> Int -> m [e]
    commitEntries :: l -> Index -> s -> m (l,s)

class (Ord t,Eq t) => LogTime t where
    logIndex :: t -> Index

{-|
Variant of 'Log' useful for implementations that need to perform 'IO'.
-}
class (Log l IO e s) => LogIO l e s
