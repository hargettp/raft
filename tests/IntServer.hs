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
    IntLogEntry(..),
    IntLog,
    newIntLog,
    newIntServer
) where

-- local imports

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
    numberLogLastCommittedIndex :: Index,
    numberLogLastAppendedIndex :: Index,
    numberLogEntries :: [RaftLogEntry]
}

instance LogIO IntLog RaftLogEntry (ServerState Int)

instance RaftLog IntLog Int

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
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
            numberLogLastAppendedIndex = index + (length newEntries) - 1,
            numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
        }
    fetchEntries log index count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries
    
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
            commit  next [] oldState = do
                return (log {
                        numberLogLastCommittedIndex = next -1
                    },oldState)
            commit next (entry:rest) oldState = do
                let newState = applyAction oldState $ entryAction entry
                commit (next + 1) rest newState

type IntServer = RaftServer IntLog Int

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