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
    Raft(..),
    mkRaft,
    RaftContext(..),
    RaftServer(..),
    RaftLog(..),
    RaftLogEntry(..),
    RaftTime(..),
    logIndex,
    RaftState(..),
    setRaftTerm,
    setRaftLeader,
    setRaftLastCandidate,
    setRaftLog,
    setRaftState
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent.STM

import Data.Serialize
import qualified Data.Set as S

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
`RaftTime` captures a measure of how up to date a log is.
-}
data RaftTime = RaftTime Term Index deriving (Show,Eq,Ord,Generic)

logIndex :: RaftTime -> Index
logIndex (RaftTime _ index) = index

instance Serialize RaftTime

{-|
A minimal 'Log' sufficient for a 'Server' to particpate in the Raft algorithm'.
-}
class (Serialize v,Log l IO RaftLogEntry (RaftState v)) => RaftLog l v where
    lastAppendedTime :: l -> RaftTime
    lastCommittedTime :: l -> RaftTime

data Raft l v = (RaftLog l v,Serialize v) => Raft {raftContext :: TVar (RaftContext l v)}

mkRaft :: (RaftLog l v) => Endpoint -> RaftServer l v -> STM (Raft l v)
mkRaft endpoint server = do
    ctx <- newTVar $ RaftContext {
        raftCurrentTerm = 0,
        raftLastCandidate = Nothing,
        raftEndpoint = endpoint,
        raftServer = server,
        raftConfigurationObservers = S.empty
    }
    return $ Raft ctx

{-|
A minimal 'Server' capable of participating in the Raft algorithm.
-}
data RaftServer l v = (RaftLog l v) => RaftServer {
    serverName :: Name,
    serverLog :: l,
    serverState :: RaftState v
}


{-|
Encapsulates the state necessary for the Raft algorithm, depending
on a 'RaftServer' for customizing the use of the algorithm to a 
specific application.
-}
data RaftContext l v = (RaftLog l v) => RaftContext {
    raftCurrentTerm :: Term,
    raftLastCandidate :: Maybe Name,
    raftEndpoint :: Endpoint,
    raftServer :: RaftServer l v,
    raftConfigurationObservers :: S.Set Name
}

{-|
Update the current term in a new 'RaftContext'
-}
setRaftTerm :: Term -> RaftContext l v -> RaftContext l v
setRaftTerm term raft = raft {
    raftCurrentTerm = term
}

{-|
Update the last candidate in a new 'RaftContext'
-}
setRaftLastCandidate :: Maybe Name -> RaftContext l v -> RaftContext l v
setRaftLastCandidate candidate raft = raft {
    raftLastCandidate = candidate
}

{-|
Update the 'RaftState' in a new 'RaftContext' to specify a new leader
-}
setRaftLeader :: Maybe Name -> RaftContext l v -> RaftContext l v
setRaftLeader leader raft = raft {
    raftServer = (raftServer raft) {
        serverState = (serverState $ raftServer raft) {
            serverConfiguration = (serverConfiguration $ serverState $ raftServer raft) {
                configurationLeader = leader
            }}}
}

setRaftLog :: (RaftLog l v) => l -> RaftContext l v -> RaftContext l v
setRaftLog rlog raft = raft {
    raftServer = (raftServer raft) {
            serverLog = rlog
        }
    }

setRaftState :: (RaftLog l v) => RaftState v -> RaftContext l v -> RaftContext l v
setRaftState state raft = raft {
    raftServer = (raftServer raft) {
            serverState = state
        }
    }

data RaftLogEntry =  RaftLogEntry {
    entryTerm :: Term,
    entryAction :: Action
} deriving (Eq,Show,Generic)

instance Serialize RaftLogEntry

data RaftState v = (Eq v, Show v) => RaftState {
    serverConfiguration :: Configuration,
    serverData :: v
}

deriving instance Eq (RaftState v)
deriving instance Show (RaftState v)

instance (State v IO Command) => State (RaftState v) IO RaftLogEntry where
    applyEntry oldRaftState entry = applyAction $ entryAction entry
        where
            applyAction (Cmd cmd) = do
                let RaftState _ oldData = oldRaftState
                newData <- applyEntry oldData cmd
                return $ oldRaftState {serverData = newData}
            applyAction action = do
                return $ oldRaftState {
                serverConfiguration = applyConfigurationAction (serverConfiguration oldRaftState) action
                }
