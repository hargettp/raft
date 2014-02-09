{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  IntServer
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- Basic log implementation for simple arithmetic on Ints, useful
-- for unit tests.
--
-----------------------------------------------------------------------------

module IntServer (
    IntCommand(..),
    IntLogEntry(..),
    IntLog
) where

-- local imports

import Data.Log

-- external imports

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data IntCommand = Add Int
    | Subtract Int
    | Multiply Int
    | Divide Int

applyCommand :: Int -> IntCommand -> Int
applyCommand initial (Add value) = initial + value
applyCommand initial (Subtract value) = initial - value
applyCommand initial (Multiply value) = initial * value
applyCommand initial (Divide value) = initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
}

data IntLog = IntLog {
    numberLogLastCommittedIndex :: Index,
    numberLogLastAppendedIndex :: Index,
    numberLogEntries :: [IntLogEntry]
}

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
        numberLogLastCommittedIndex = -1,
        numberLogLastAppendedIndex = -1,
        numberLogEntries = []
    }

instance Log IntLog IO IntLogEntry Int where
    newLog = newIntLog
    -- lastCommitted :: l -> m Index
    lastCommitted log = numberLogLastCommittedIndex log
    -- lastAppended :: l -> m Index
    lastAppended log = numberLogLastAppendedIndex log
    -- appendEntries :: l -> Index -> [e s] -> m l
    appendEntries log index newEntries = do
        -- TODO cleanup this logic
        return log {
            numberLogLastAppendedIndex = index + (length newEntries) - 1,
            numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
        }
    -- fetchEntries :: l -> Index -> Int -> m [e s]
    fetchEntries log index count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries
    -- commitEntries :: l -> Index -> s -> m (l,s)
    commitEntries log index state = do
        let committed = numberLogLastCommittedIndex log
        if index > committed
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetch nextCommitted (index - committed)
                commit nextCommitted uncommitted state
            else return (log,state)
        where
            fetch start count = do
                let existing = numberLogEntries log
                return $ take count $ drop start existing
            -- commit :: Index -> [IntLogEntry Int] -> Int -> STM Int
            commit  next [] oldState = do
                return (log {
                        numberLogLastCommittedIndex = next -1
                    },oldState)
            commit next (entry:rest) oldState = do
                let newState = applyCommand oldState $ entryCommand entry
                commit (next + 1) rest newState
