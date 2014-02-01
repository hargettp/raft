{-# LANGUAGE MultiParamTypeClasses #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  NumberServer
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

module NumberServer (
    LogEntry(..),
    NumberLog
) where

-- local imports

import Data.Log

-- external imports

import Control.Concurrent.STM

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type Action a = a -> a

data LogEntry a = LogEntry {
    entryAction :: Action a
}

instance Log NumberLog IO LogEntry Int where
    newLog = newNumberLog
    -- lastCommitted :: l -> m Index
    lastCommitted log = atomically $ readTVar $ numberLogLastCommittedIndex log
    -- lastAppended :: l -> m Index
    lastAppended log = atomically $ readTVar $ numberLogLastAppendedIndex log
    -- appendEntries :: l -> Index -> [e s] -> m ()
    appendEntries log index newEntries = atomically $ do
        modifyTVar (numberLogEntries log) $ \oldEntries -> (take index oldEntries) ++ newEntries
        modifyTVar (numberLogLastAppendedIndex log) $ \oldAppended -> oldAppended + (length newEntries)
        return ()
    -- fetchEntries :: l -> Index -> Int -> m [e s]
    fetchEntries log index count = atomically $ do
        entries <- readTVar $ numberLogEntries log
        return $ take count $ drop index entries
    -- commitEntries :: l -> Index -> s -> m s
    commitEntries log index state = atomically $ do
        let committedIndex = numberLogLastCommittedIndex log
        committed <- readTVar committedIndex
        if index > committed
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetch nextCommitted (index - committed)
                commit committedIndex nextCommitted uncommitted state
            else return state
        where
            fetch start count = do
                existing <- readTVar $ numberLogEntries log
                return $ take count $ drop start existing
            commit :: TVar Index -> Index -> [LogEntry Int] -> Int -> STM Int
            commit committedIndex next [] oldState = do
                writeTVar committedIndex $ next - 1
                return oldState
            commit committedIndex next (entry:rest) oldState = do
                let newState = (entryAction entry) oldState
                commit committedIndex (next + 1) rest newState


data NumberLog = NumberLog {
    numberLogLastCommittedIndex :: TVar Index,
    numberLogLastAppendedIndex :: TVar Index,
    numberLogEntries :: TVar [LogEntry Int]
}

newNumberLog :: IO NumberLog
newNumberLog = do 
    committed <- atomically $ newTVar $ -1
    appended <- atomically $ newTVar $ -1
    entries <- atomically $ newTVar []
    return NumberLog {
        numberLogLastCommittedIndex = committed,
        numberLogLastAppendedIndex = appended,
        numberLogEntries = entries
    }
