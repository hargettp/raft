{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Log
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

module Data.Log (

    Index,
    Log(..)

) where

-- local imports

-- external imports

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-
A log of type @l@ is a sequence of entries of type @e@ such that
entries can be appended to the log starting at a particular 'Index' (and potentially
overwrite entries previously appended at the same index), fetched
from a particular 'Index', or committed up to a certain 'Index'. Once committed,
it is undefined whether attempting to fetch entries with an 'Index' < 'lastCommitted'
will succeed or throw an error, as some log implementations may throw away some
committed entries.

Each entry in the index defines an action that transforms a supplied initial state
into a new state.  Commiting log, given some initial state, applies the action contained in
each entry in sequence (starting at a specified 'Index') to some state of type @s@, 
producing a new state after committing as many entries as possible.

Each log implementation may choose the monad @m@ in which they operate.
-}
class Log l m e s | l -> e,l -> s where
    newLog :: m l
    lastCommitted :: l -> m Index
    lastAppended :: l -> m Index
    appendLog :: l -> Index -> [e s] -> m ()
    fetchEntries :: l -> Index -> Int -> m [e s]
    commitLog :: l -> Index -> s -> m s

{-
An index is a logical offset into a log.
-}
type Index = Int
    