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
    IntState(..),
    mkIntLog,
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

data IntState = IntState Int deriving (Eq,Show)

{-
applyAction :: Int -> Command -> Int
applyAction initial cmd = let Right icmd = decode cmd
                               in applyIntCommand initial icmd
applyAction initial action = initial {
    serverConfiguration = applyConfigurationAction (serverConfiguration initial) action
    }
-}

applyIntCommand :: IntState -> IntCommand -> IO IntState
applyIntCommand (IntState initial) (Add value) = return $ IntState $ initial + value
applyIntCommand (IntState initial) (Subtract value) = return $ IntState $ initial - value
applyIntCommand (IntState initial) (Multiply value) = return $ IntState $ initial  * value
applyIntCommand (IntState initial) (Divide value) = return $ IntState $ initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Generic)

instance Serialize IntLogEntry

data IntLog = IntLog {
    numberLogLastCommitted :: RaftTime,
    numberLogLastAppended :: RaftTime,
    numberLogEntries :: [RaftLogEntry]
}

instance RaftLog IntLog IntState where
    lastAppendedTime = numberLogLastAppended
    lastCommittedTime = numberLogLastCommitted

mkIntLog :: IO IntLog
mkIntLog = do
    return IntLog {
        numberLogLastCommitted = RaftTime (-1) (-1),
        numberLogLastAppended = RaftTime (-1) (-1),
        numberLogEntries = []
    }

instance State IntState IO Command where

    applyEntry initial cmd = do
        let Right icmd = decode cmd
        applyIntCommand initial icmd

instance Log IntLog IO RaftLogEntry (RaftState IntState) where

    mkLog = mkIntLog

    lastCommitted log = logIndex $ numberLogLastCommitted log

    lastAppended log = logIndex $ numberLogLastAppended log

    appendEntries log index newEntries = do
        if null newEntries
            then return log
            else do
                let term = maximum $ map entryTerm newEntries
                return log {
                    numberLogLastAppended = RaftTime term (index + (length newEntries) - 1),
                    numberLogEntries = (take (index + 1) (numberLogEntries log)) ++ newEntries
                }
    fetchEntries log index count = do
        let entries = numberLogEntries log
        return $ take count $ drop index entries

    commitEntry oldLog commitIndex entry = do
        let newLog = oldLog {
                numberLogLastCommitted = RaftTime (entryTerm entry) commitIndex
                }
        return newLog

type IntServer = RaftServer IntLog IntState

type IntRaft = Raft IntLog IntState

mkIntServer :: Configuration -> Name -> Int -> IO IntServer
mkIntServer cfg sid initial = do
    log <- mkIntLog
    return RaftServer {
        serverName = sid,
        serverLog = log,
        serverState = RaftState {
            serverConfiguration = cfg,
            serverData = IntState initial
            }
    }