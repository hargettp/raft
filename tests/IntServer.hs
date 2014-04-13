{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
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
    IntRaft,
    IntLogEntry(..),
    IntLog,
    newIntLog,
    IntServer,
    newIntServer
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Types

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

applyAction :: ServerState Int -> Action -> ServerState Int
applyAction initial (Cmd cmd) = let Right icmd = decode cmd
                               in applyIntCommand initial icmd
applyAction initial action = initial {
    serverConfiguration = applyConfigurationAction (serverConfiguration initial) action
    }

applyIntCommand :: ServerState Int -> IntCommand -> ServerState Int
applyIntCommand initial (Add value) = initial {serverData = (serverData initial) + value}
applyIntCommand initial (Subtract value) = initial {serverData = (serverData initial) - value}
applyIntCommand initial (Multiply value) = initial {serverData = (serverData initial) * value}
applyIntCommand initial (Divide value) = initial {serverData = (serverData initial) `quot` value}

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Generic)

instance Serialize IntLogEntry

data IntLog = IntLog {
    numberLogLastTerm :: Term,
    numberLogLastCommittedIndex :: Index,
    numberLogLastAppendedIndex :: Index,
    numberLogEntries :: [RaftLogEntry]
}

instance LogIO IntLog RaftLogEntry (ServerState Int)

instance RaftLog IntLog Int where
    -- logLastAppendedTime :: l -> RaftTime
    logLastAppendedTime log = RaftTime (numberLogLastTerm log) (numberLogLastAppendedIndex log)

    -- logLastCommittedTime :: l -> RaftTime
    logLastCommittedTime log = RaftTime (numberLogLastTerm log) (numberLogLastCommittedIndex log)

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
        numberLogLastTerm = -1,
        numberLogLastCommittedIndex = -1,
        numberLogLastAppendedIndex = -1,
        numberLogEntries = []
    }

instance Log IntLog IO RaftLogEntry (ServerState Int) where

    newLog = newIntLog

    lastCommitted log = numberLogLastCommittedIndex log

    lastAppended log = numberLogLastAppendedIndex log

    appendEntries log index newEntries = do
        -- TODO cleanup this logic
        return log {
            numberLogLastTerm = maximum $ map entryTerm newEntries,
            numberLogLastAppendedIndex = index + (length newEntries) - 1,
            numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
        }
    fetchEntries log index count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries

    commitEntries log index initialState = do
        let committed = numberLogLastCommittedIndex log
        if index > committed
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetch nextCommitted (index - committed)
                commit nextCommitted uncommitted initialState
            else return (log,initialState)
        where
            fetch start count = do
                let existing = numberLogEntries log
                return $ take count $ drop start existing
            commit  nextCommitted [] oldState = do
                return (log {
                        numberLogLastCommittedIndex = nextCommitted -1
                    },oldState)
            commit nextCommitted (entry:rest) oldState = do
                let state = applyAction oldState $ entryAction entry
                (committedLog,committedState) <- commit (nextCommitted + 1)  rest state
                return (committedLog {numberLogLastTerm = max (numberLogLastTerm log) (entryTerm entry)},
                            committedState)

type IntServer = RaftServer IntLog Int

type IntRaft = Raft IntLog Int

newIntServer :: Configuration -> ServerId -> Int -> IO IntServer
newIntServer cfg sid initial = do
    log <- newIntLog
    return Server {
        serverId = sid,
        serverLog = log,
        serverState = ServerState {
            serverConfiguration = cfg,
            serverData = initial
            }
    }