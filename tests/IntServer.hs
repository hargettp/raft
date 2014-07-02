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
    IntLog(..),
    IntState(..),
    mkIntLog
) where

-- local imports

import Control.Consensus.Raft

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
    deriving (Eq,Show,Generic)

instance Serialize IntCommand

data IntState = IntState Int deriving (Eq,Show,Generic,Ord)

instance Serialize IntState

applyIntCommand :: IntState -> IntCommand -> IO IntState
applyIntCommand (IntState initial) (Add value) = return $ IntState $ initial + value
applyIntCommand (IntState initial) (Subtract value) = return $ IntState $ initial - value
applyIntCommand (IntState initial) (Multiply value) = return $ IntState $ initial  * value
applyIntCommand (IntState initial) (Divide value) = return $ IntState $ initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Eq,Show,Generic)

instance Serialize IntLogEntry

data IntLog = IntLog {
    numberLogLastCommitted :: RaftTime,
    numberLogLastAppended :: RaftTime,
    numberLogEntries :: [RaftLogEntry IntCommand]
} deriving (Eq,Show)

instance RaftLog IntLog IntCommand IntState where
    lastAppendedTime = numberLogLastAppended
    lastCommittedTime = numberLogLastCommitted

mkIntLog :: IO IntLog
mkIntLog = do
    return IntLog {
        numberLogLastCommitted = RaftTime (-1) (-1),
        numberLogLastAppended = RaftTime (-1) (-1),
        numberLogEntries = []
    }

instance State IntState IO IntCommand where

    canApplyEntry _ _ = return True

    applyEntry initial cmd = do
        applyIntCommand initial cmd

instance Log IntLog IO (RaftLogEntry IntCommand) (RaftState IntState) where

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

type IntRaft = Raft IntLog IntCommand IntState
