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
import Control.Consensus.Raft.Types

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
    numberLogLastTerm :: Term,
    numberLogLastCommitted :: RaftTime,
    numberLogLastAppended :: RaftTime,
    numberLogEntries :: [RaftLogEntry]
}

instance LogIO IntLog RaftTime RaftLogEntry (ServerState Int)

instance RaftLog IntLog Int

newIntLog :: IO IntLog
newIntLog = do
    return IntLog {
        numberLogLastTerm = -1,
        numberLogLastCommitted = RaftTime (-1) (-1),
        numberLogLastAppended = RaftTime (-1) (-1),
        numberLogEntries = []
    }

instance Log IntLog RaftTime IO RaftLogEntry (ServerState Int) where

    mkLog = newIntLog

    lastCommitted log = numberLogLastCommitted log

    lastAppended log = numberLogLastAppended log

    appendEntries log (RaftTime term index) newEntries = do
        -- TODO cleanup this logic
        return log {
            numberLogLastTerm = maximum $ map entryTerm newEntries,
            numberLogLastAppended = RaftTime term (index + (length newEntries) - 1),
            numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
        }
    fetchEntries log (RaftTime _ index) count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries

    commitEntries log (RaftTime term index) initialState = do
        let RaftTime _ committed = numberLogLastCommitted log
            count = index - committed
        if count > 0
            then do
                let nextCommitted = committed + 1
                uncommitted <- fetchEntries log (RaftTime term nextCommitted) count
                commit nextCommitted uncommitted initialState
            else return (log,initialState)
        where
            commit nextCommitted [] oldState = do
                return (log {
                        numberLogLastCommitted = RaftTime term (nextCommitted - 1)
                    },oldState)
            commit nextCommitted (entry:rest) oldState = do
                let state = applyAction oldState $ entryAction entry
                (committedLog,committedState) <- commit (nextCommitted + 1)  rest state
                return (committedLog {numberLogLastTerm = max (numberLogLastTerm log) (entryTerm entry)},
                            committedState)

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