{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}

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
    IntLog,
    newIntLog,
    newIntServer
) where

-- local imports

import Control.Consensus.Raft
import Control.Consensus.Raft.Types

import Data.Log

-- external imports

import Prelude hiding (log)

import Data.Serialize

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data IntCommand = Add Int
    | Subtract Int
    | Multiply Int
    | Divide Int
    deriving (Generic)

instance Serialize IntCommand

applyAction :: Int -> Action -> Int
-- TODO fix this later--should be applied to server, not log or state
applyAction initial (Cfg _) = initial
applyAction initial (Cmd cmd) = let Right icmd = decode cmd
                               in applyIntCommand initial icmd

applyIntCommand :: Int -> IntCommand -> Int
applyIntCommand initial (Add value) = initial + value
applyIntCommand initial (Subtract value) = initial - value
applyIntCommand initial (Multiply value) = initial * value
applyIntCommand initial (Divide value) = initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Generic)

instance Serialize IntLogEntry

data IntLog = IntLog {
    numberLogLastCommittedIndex :: Index,
    numberLogLastAppendedIndex :: Index,
    numberLogEntries :: [RaftLogEntry]
}

instance LogIO IntLog RaftLogEntry Int

instance RaftLog IntLog Int

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
        numberLogLastCommittedIndex = -1,
        numberLogLastAppendedIndex = -1,
        numberLogEntries = []
    }

-- instance Log IntLog IO IntLogEntry Int where
instance Log IntLog IO RaftLogEntry Int where
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
                let newState = applyAction oldState $ entryAction entry
                commit (next + 1) rest newState

-- type IntServer = Server IntLog IntLogEntry Int
type IntServer = RaftServer IntLog Int

newIntServer :: Configuration -> ServerId -> Int -> IO IntServer
newIntServer cfg sid initial = do
    log <- newIntLog
    return Server {
        serverId = sid,
        serverConfiguration = cfg,
        serverLog = log,
        serverState = initial
    }