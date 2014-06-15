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
    RaftLog(..),
    RaftLogEntry(..),
    RaftState(..),
    mkRaftState,
    setRaftTerm,
    setRaftLeader,
    isRaftLeader,
    setRaftLastCandidate,
    setRaftConfiguration,
    setRaftConfigurationIndex,
    raftConfiguration,
    raftMembers,
    raftSafeAppendedTerm,
    setRaftMembers,
    setRaftLog,
    setRaftState
) where

-- local imports

import Data.Log
import Control.Consensus.Raft.Actions
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Serialize

import GHC.Generics

import Network.Endpoints

import System.Log.Logger

import Text.Printf

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

{-|
Encapsulates the state necessary for the Raft algorithm, depending
on a 'RaftServer' for customizing the use of the algorithm to a 
specific application.
-}
data RaftContext l v = (RaftLog l v) => RaftContext {
    raftEndpoint :: Endpoint,
    raftLog :: l,
    raftState :: RaftState v
}

data Raft l v = (RaftLog l v,Serialize v) => Raft {raftContext :: TVar (RaftContext l v)}

mkRaft :: (RaftLog l v) => Endpoint -> l -> RaftState v -> STM (Raft l v)
mkRaft endpoint initialLog initialState = do
    ctx <- newTVar $ RaftContext {
        raftEndpoint = endpoint,
        raftLog = initialLog,
        raftState = initialState
    }
    return $ Raft ctx

{-|
A minimal 'Log' sufficient for a 'Server' to particpate in the Raft algorithm'.
-}
class (Eq v,Show v,Serialize v,Log l IO RaftLogEntry (RaftState v)) => RaftLog l v where
    lastAppendedTime :: l -> RaftTime
    lastCommittedTime :: l -> RaftTime


data RaftState v = (Eq v, Show v) => RaftState {
    raftStateCurrentTerm :: Term,
    raftStateLastCandidate :: Maybe Name,
    raftStateName :: Name,
    raftStateConfigurationIndex :: Maybe Index,
    raftStateConfiguration :: RaftConfiguration,
    raftStateMembers :: Members,
    raftStateData :: v
}

mkRaftState :: (Eq v, Show v) => v -> RaftConfiguration -> Name -> RaftState v
mkRaftState initialData cfg name = RaftState {
    raftStateCurrentTerm = 0,
    raftStateLastCandidate = Nothing,
    raftStateName = name,
    raftStateConfigurationIndex = Nothing,
    raftStateConfiguration = cfg,
    raftStateMembers = mkMembers cfg initialRaftTime,
    raftStateData = initialData
}

deriving instance Eq (RaftState v)
deriving instance Show (RaftState v)

data RaftLogEntry =  RaftLogEntry {
    entryTerm :: Term,
    entryAction :: Action
} deriving (Eq,Show,Generic)

instance Serialize RaftLogEntry

instance (State v IO Command) => State (RaftState v) IO RaftLogEntry where
    canApplyEntry oldRaftState index entry = do
        let members = raftStateMembers oldRaftState
            cfg = raftStateConfiguration oldRaftState
            term = membersSafeAppendedTerm members $ clusterConfiguration cfg
            currentTerm = raftStateCurrentTerm oldRaftState
            leader = (Just $ raftStateName oldRaftState) == (clusterLeader $ clusterConfiguration cfg)
        infoM _log $ printf "%v: Safe term for members %v is %v" currentTerm (show $ M.map (logTerm . memberLogLastAppended) members) term
        if leader
                then if term /= raftStateCurrentTerm oldRaftState
                    then return False
                    else canApply $ entryAction entry
                else canApply $ entryAction entry
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
                let cfg = applyConfigurationAction (clusterConfiguration $ raftStateConfiguration oldRaftState) action
                infoM _log $ printf "New configuration is %v" (show cfg)
                return $ oldRaftState {
                    raftStateConfiguration = (raftStateConfiguration oldRaftState) {
                        clusterConfiguration = cfg
                    }
                }

raftCurrentTerm :: (RaftLog l v) => RaftContext l v -> Term
raftCurrentTerm raft = raftStateCurrentTerm $ raftState raft

raftName :: (RaftLog l v) => RaftContext l v -> Name
raftName raft = raftStateName $ raftState raft

{-|
Update the current term in a new 'RaftContext'
-}
setRaftTerm :: Term -> RaftContext l v -> RaftContext l v
setRaftTerm term raft = raft {
        raftState = (raftState raft) {
            raftStateCurrentTerm = term
            }
        }

{-|
Update the current term in a new 'RaftContext'
-}
setRaftMembers :: Members -> RaftContext l v -> RaftContext l v
setRaftMembers members raft = raft {
        raftState = (raftState raft) {
            raftStateMembers = members
            }
        }

raftMembers :: (RaftLog l v) => RaftContext l v -> Members
raftMembers raft = raftStateMembers $ raftState raft

raftSafeAppendedTerm :: (RaftLog l v) => RaftContext l v -> Term
raftSafeAppendedTerm raft = 
    let members = raftMembers raft
        cfg = raftConfiguration raft
        in membersSafeAppendedTerm members cfg

{-|
Update the last candidate in a new 'RaftContext'
-}
setRaftLastCandidate :: Maybe Name -> RaftContext l v -> RaftContext l v
setRaftLastCandidate candidate raft = raft {
                    raftState = (raftState raft) {
                        raftStateLastCandidate = candidate
                        }
                    }

{-|
Update the 'RaftState' in a new 'RaftContext' to specify a new leader
-}
setRaftLeader :: Maybe Name -> RaftContext l v -> RaftContext l v
setRaftLeader leader raft = 
    let cfg = clusterConfiguration $ raftStateConfiguration $ raftState raft
        in case cfg of
            Configuration _ _ _ -> raft {
                    raftState = (raftState raft) {
                        raftStateConfiguration = (raftStateConfiguration $ raftState raft) {
                            clusterConfiguration = cfg {
                            configurationLeader = leader
                        }}}
                }
            JointConfiguration _ jointNew -> raft {
                    raftState = (raftState raft) {
                        raftStateConfiguration = (raftStateConfiguration $ raftState raft) {
                            clusterConfiguration = jointNew {
                            configurationLeader = leader
                        }}}
                }

isRaftLeader :: (RaftLog l v) => RaftContext l v -> Bool
isRaftLeader raft = (Just $ raftName raft) == (clusterLeader $ raftConfiguration raft)

setRaftLog :: (RaftLog l v) => l -> RaftContext l v -> RaftContext l v
setRaftLog rlog raft = raft {
        raftLog = rlog
        }

setRaftConfiguration :: (RaftLog l v) => Configuration -> RaftContext l v -> RaftContext l v
setRaftConfiguration cfg raft =
    let newState = (raftState raft) {
        raftStateConfiguration = (raftStateConfiguration $ raftState raft) {
                            clusterConfiguration = cfg
        }}
        in setRaftState newState raft

raftConfiguration :: (RaftLog l v) => RaftContext l v -> Configuration 
raftConfiguration raft = clusterConfiguration $ raftStateConfiguration $ raftState raft

setRaftConfigurationIndex :: (RaftLog l v) => Maybe Index -> RaftContext l v -> RaftContext l v
setRaftConfigurationIndex index raft =
    let newState = (raftState raft) {
        raftStateConfigurationIndex = index
        }
        in setRaftState newState raft

setRaftState :: (RaftLog l v) => RaftState v -> RaftContext l v -> RaftContext l v
setRaftState state raft = raft {
        raftState = state
        }
