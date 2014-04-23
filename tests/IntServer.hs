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
    mkIntServer
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Log

-- external imports

import Prelude hiding (log)

import Data.Serialize

import GHC.Generics

import Network.Endpoints

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
    numberLogLastCommitted :: RaftTime,
    numberLogLastAppended :: RaftTime,
    numberLogEntries :: [RaftLogEntry]
}

instance RaftLog IntLog Int where
    lastAppendedTime = numberLogLastAppended
    lastCommittedTime = numberLogLastCommitted

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
        numberLogLastCommitted = RaftTime (-1) (-1),
        numberLogLastAppended = RaftTime (-1) (-1),
        numberLogEntries = []
    }

instance Log IntLog IO RaftLogEntry (ServerState Int) where

    mkLog = newIntLog

    lastCommitted log = logIndex $ numberLogLastCommitted log

    lastAppended log = logIndex $ numberLogLastAppended log

    appendEntries log index newEntries = do
        let term = maximum $ map entryTerm newEntries
        return log {
            numberLogLastAppended = RaftTime term (index + (length newEntries) - 1),
            numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
        }
    fetchEntries log index count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries

    commitEntries initialLog index initialState = do
        let committed = logIndex $ numberLogLastCommitted initialLog
            count = index - committed
        if count > 0
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetchEntries initialLog nextCommitted count
                commit initialLog initialState nextCommitted uncommitted 
            else return (initialLog,initialState)
        where
            applyEntry oldState entry = applyAction oldState $ entryAction entry
            commitEntry oldLog commitIndex entry =
                let newLog = oldLog {
                        numberLogLastCommitted = RaftTime (entryTerm entry) commitIndex
                        }
                    in newLog
            commit oldLog oldState _ [] = do
                return (oldLog,oldState)
            commit oldLog oldState commitIndex (entry:rest) = do
                let newState = applyEntry oldState entry
                    newLog = commitEntry oldLog commitIndex entry
                commit newLog newState (commitIndex + 1)  rest

type IntServer = RaftServer IntLog Int

type IntRaft = Raft IntLog Int

mkIntServer :: Configuration -> Name -> Int -> IO IntServer
mkIntServer cfg sid initial = do
    log <- newIntLog
    return Server {
        serverName = sid,
        serverLog = log,
        serverState = ServerState {
            serverConfiguration = cfg,
            serverData = initial
            }
    }