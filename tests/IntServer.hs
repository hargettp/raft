{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}

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
import Control.Consensus.Raft.Actions
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
    deriving (Generic,Show)

instance Serialize IntCommand

data IntState = IntState Int deriving (Eq,Show,Generic)

instance Serialize IntState

applyIntCommand :: IntState -> IntCommand -> IO IntState
applyIntCommand (IntState initial) (Add value) = return $ IntState $ initial + value
applyIntCommand (IntState initial) (Subtract value) = return $ IntState $ initial - value
applyIntCommand (IntState initial) (Multiply value) = return $ IntState $ initial  * value
applyIntCommand (IntState initial) (Divide value) = return $ IntState $ initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Generic,Show)

instance Serialize IntLogEntry

data IntLog = IntLog {
    numberLogLastCommitted :: RaftTime,
    numberLogLastAppended :: RaftTime,
    numberLogEntries :: [RaftLogEntry]
} deriving (Show)

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
    -- canApplyEntry :: s -> Index -> e -> m Bool
    canApplyEntry _ _ _ = return True

    applyEntry initial _ cmd = do
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

    checkpoint oldLog oldState = return (oldLog,oldState)

type IntServer = RaftServer IntLog IntState

deriving instance Show IntServer

type IntRaft = Raft IntLog IntState

mkIntServer :: Configuration -> Name -> Int -> IO IntServer
mkIntServer cfg name initial = do
    log <- mkIntLog
    return RaftServer {
        raftServerLog = log,
        raftServerState = mkRaftState (IntState initial) cfg name
    }