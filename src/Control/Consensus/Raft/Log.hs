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
    raftCurrentTerm,
    raftName,
    RaftContext(..),
    RaftServer(..),
    RaftLog(..),
    RaftLogEntry(..),
    RaftState(..),
    mkRaftState,
    setRaftTerm,
    setRaftLeader,
    isRaftLeader,
    setRaftLastCandidate,
    setRaftConfiguration,
    raftConfiguration,
    raftMembers,
    setRaftMembers,
    setRaftLog,
    setRaftState
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent.STM

import Data.Serialize
import qualified Data.Set as S

import GHC.Generics

import Network.Endpoints

import System.Log.Logger

import Text.Printf

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

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
    raftLastCandidate :: Maybe Name,
    raftEndpoint :: Endpoint,
    raftServer :: RaftServer l v,
    raftConfigurationObservers :: S.Set Name
}

raftCurrentTerm :: (RaftLog l v) => RaftContext l v -> Term
raftCurrentTerm raft = raftStateCurrentTerm $ serverState $ raftServer raft

raftName :: (RaftLog l v) => RaftContext l v -> Name
raftName raft = serverName $ raftServer raft

{-|
Update the current term in a new 'RaftContext'
-}
setRaftTerm :: Term -> RaftContext l v -> RaftContext l v
setRaftTerm term raft = raft {
    raftServer = (raftServer raft) {
        serverState = (serverState $ raftServer raft) {
            raftStateCurrentTerm = term
        }
    }
}

{-|
Update the current term in a new 'RaftContext'
-}
setRaftMembers :: Members -> RaftContext l v -> RaftContext l v
setRaftMembers members raft = raft {
    raftServer = (raftServer raft) {
        serverState = (serverState $ raftServer raft) {
            raftStateMembers = members
        }
    }
}

raftMembers :: (RaftLog l v) => RaftContext l v -> Members
raftMembers raft = raftStateMembers $ serverState $ raftServer raft

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
setRaftLeader leader raft = 
    let cfg = raftStateConfiguration $ serverState $ raftServer raft
        in case cfg of
            Configuration _ _ _ _ -> raft {
                raftServer = (raftServer raft) {
                    serverState = (serverState $ raftServer raft) {
                        raftStateConfiguration = cfg {
                            configurationLeader = leader
                        }}}
                }
            JointConfiguration _ jointNew -> raft {
                raftServer = (raftServer raft) {
                    serverState = (serverState $ raftServer raft) {
                        raftStateConfiguration = jointNew {
                            configurationLeader = leader
                        }}}
                }

isRaftLeader :: (RaftLog l v) => RaftContext l v -> Bool
isRaftLeader raft = (Just $ raftName raft) == (clusterLeader $ raftConfiguration raft)

setRaftLog :: (RaftLog l v) => l -> RaftContext l v -> RaftContext l v
setRaftLog rlog raft = raft {
    raftServer = (raftServer raft) {
            serverLog = rlog
        }
    }

setRaftConfiguration :: (RaftLog l v) => Configuration -> RaftContext l v -> RaftContext l v
setRaftConfiguration cfg raft =
    let newState = (serverState $ raftServer raft) {
        raftStateConfiguration = cfg
        }
        in setRaftState newState raft

raftConfiguration :: (RaftLog l v) => RaftContext l v -> Configuration 
raftConfiguration raft = raftStateConfiguration $ serverState $ raftServer raft

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
    raftStateCurrentTerm :: Term,
    raftStateNewParticipants :: Maybe (Index,[Name]),
    raftStateName :: Name,
    raftStateConfiguration :: Configuration,
    raftStateMembers :: Members,
    raftStateData :: v
}

mkRaftState :: (Eq v, Show v) => v -> Name -> RaftState v
mkRaftState initialData name = let cfg = newConfiguration [] in RaftState {
    raftStateCurrentTerm = 0,
    raftStateNewParticipants = Nothing,
    raftStateName = name,
    raftStateConfiguration = cfg,
    raftStateMembers = mkMembers cfg $ RaftTime (-1) (-1),
    raftStateData = initialData
}

deriving instance Eq (RaftState v)
deriving instance Show (RaftState v)

instance (State v IO Command) => State (RaftState v) IO RaftLogEntry where
    canApplyEntry oldRaftState index entry = 
        {-
        let members = raftStateMembers oldRaftState
            cfg = raftStateConfiguration oldRaftState
            term = membersSafeAppendedTerm members cfg
            leader = (Just $ raftStateName oldRaftState) == (clusterLeader cfg)
            in if leader 
                then if term /= raftStateCurrentTerm oldRaftState
                    then return False
                    else canApply $ entryAction entry
                else canApply $ entryAction entry
        -}
        canApply $ entryAction entry
        where
            canApply (Cmd cmd) = do
                let oldData = raftStateData oldRaftState
                canApplyEntry oldData index cmd
            -- TODO check configuration cases
            canApply _ = return True

    applyEntry oldRaftState index entry = applyAction $ entryAction entry
        where
            applyAction (Cmd cmd) = do
                let oldData = raftStateData oldRaftState
                newData <- applyEntry oldData index cmd
                return $ oldRaftState {raftStateData = newData}
            applyAction action = do
                let cfg = applyConfigurationAction (raftStateConfiguration oldRaftState) action
                infoM _log $ printf "New configuration is %v" (show cfg)
                return $ oldRaftState {
                    raftStateNewParticipants =
                        if isJointConfiguration cfg
                            then Just (index,S.toList $ configurationParticipants $ jointNewConfiguration cfg)
                            else Nothing,
                    raftStateConfiguration = cfg
                }
