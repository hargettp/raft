{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.State
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- (..... module description .....)
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft.Log (
    -- * Raft state
    Raft,
    newRaft,
    RaftState(..),
    RaftServer,
    RaftLog,
    RaftLogEntry(..),
    RaftTime(..),
    ServerState(..),
    setRaftTerm,
    setRaftLeader,
    setRaftLastCandidate,
    setRaftLog,
    setRaftServerState
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent.STM

import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
`RaftTime` captures a measure of how up to date a log is.
-}
data RaftTime = RaftTime Term Index deriving (Show,Eq,Ord,Generic)

instance LogTime RaftTime where
    logIndex (RaftTime _ index) = index
    nextLogTime (RaftTime term index) = RaftTime term (index + 1)

instance Serialize RaftTime

{-|
A minimal 'Log' sufficient for a 'Server' to particpate in the Raft algorithm'.
-}
class (LogIO l RaftTime RaftLogEntry (ServerState v)) => RaftLog l v

type Raft l v = TVar (RaftState l v)

newRaft :: (RaftLog l v) => RaftServer l v -> STM (Raft l v)
newRaft server = newTVar $ RaftState {
        raftCurrentTerm = 0,
        raftLastCandidate = Nothing,
        raftServer = server
    }

{-|
A minimal 'Server' capable of participating in the Raft algorithm.
-}
type RaftServer l v = Server l RaftTime RaftLogEntry (ServerState v)

{-|
Encapsulates the state necessary for the Raft algorithm, depending
on a 'RaftServer' for customizing the use of the algorithm to a 
specific application.
-}
data RaftState l v = (RaftLog l v) => RaftState {
    raftCurrentTerm :: Term,
    raftLastCandidate :: Maybe Name,
    raftServer :: RaftServer l v
}

{-|
Update the current term in a new 'RaftState'
-}
setRaftTerm :: Term -> RaftState l v -> RaftState l v
setRaftTerm term raft = raft {
    raftCurrentTerm = term
}

{-|
Update the last candidate in a new 'RaftState'
-}
setRaftLastCandidate :: Maybe Name -> RaftState l v -> RaftState l v
setRaftLastCandidate candidate raft = raft {
    raftLastCandidate = candidate
}

{-|
Update the 'ServerState' in a new 'RaftState' to specify a new leader
-}
setRaftLeader :: Maybe Name -> RaftState l v -> RaftState l v
setRaftLeader leader raft = raft {
    raftServer = (raftServer raft) {
        serverState = (serverState $ raftServer raft) {
            serverConfiguration = (serverConfiguration $ serverState $ raftServer raft) {
                configurationLeader = leader
            }}}
}

setRaftLog :: (RaftLog l v) => l -> RaftState l v -> RaftState l v
setRaftLog rlog raft = raft {
    raftServer = (raftServer raft) {
            serverLog = rlog
        }
    }

setRaftServerState :: (RaftLog l v) => ServerState v -> RaftState l v -> RaftState l v
setRaftServerState state raft = raft {
    raftServer = (raftServer raft) {
            serverState = state
        }
    }

data RaftLogEntry =  RaftLogEntry {
    entryTerm :: Term,
    entryAction :: Action
} deriving (Eq,Show,Generic)

instance Serialize RaftLogEntry

data ServerState v = (Eq v,Show v) => ServerState {
    serverConfiguration :: Configuration,
    serverData :: v
}

deriving instance Eq (ServerState v)
deriving instance Show (ServerState v)