{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
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
-- This module defines the base extensions to the fundamental 'Data.Log.Log' and 'Data.Log.State'
-- types in order to support the Raft algorithm.  For example, in ordinary 'Data.Log.Log's,
-- there are no constraints on the entries that change the 'Data.Log.State' of the underlying
-- state machine. For Raft, however, such entries must be capable of declaring the 'Term'
-- in which the entry was created.  Thus, a 'RaftLog' uses a 'RaftLogEntry' as the type for
-- its entries.
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
    setRaftReady,
    isRaftReady,
    setRaftLastCandidate,
    setRaftConfiguration,
    raftConfiguration,
    raftMembers,
    raftSafeAppendedTerm,
    setRaftMembers,
    setRaftLog,
    setRaftState,

    ListLog(..),
    mkListLog,

    module Control.Consensus.Raft.Actions,
    module Control.Consensus.Raft.Types,
    module Data.Log

) where

-- local imports

import Data.Log
import Control.Consensus.Raft.Actions
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Serialize

import Network.Endpoints

import Prelude hiding (log)

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
data RaftContext l e v = (RaftLog l e v) => RaftContext {
    raftEndpoint :: Endpoint,
    raftLog :: l,
    raftState :: RaftState v
}

{-|
Encapsulates the complete state necessary for participating in the Raft algorithm,
in a mutable form by storing it in a 'TVar'.
-}
data Raft l e v = (RaftLog l e v,Serialize v) => Raft {raftContext :: TVar (RaftContext l e v)}

{-|
Create a new 'Raft' instance.
-}
mkRaft :: (RaftLog l e v) => Endpoint -> l -> RaftState v -> STM (Raft l e v)
mkRaft endpoint initialLog initialState = do
    ctx <- newTVar $ RaftContext {
        raftEndpoint = endpoint,
        raftLog = initialLog,
        raftState = initialState
    }
    return $ Raft ctx

{-|
A minimal 'Log' sufficient for a member to particpate in the Raft algorithm'.
-}
class (Serialize e,Serialize v,Log l IO (RaftLogEntry e) (RaftState v)) => RaftLog l e v where
    lastAppendedTime :: l -> RaftTime
    lastCommittedTime :: l -> RaftTime

{-|
The 'State' that 'RaftLog's expect for participating in the Raft algorithm.
-}
data RaftState v = (Serialize v) => RaftState {
    raftStateCurrentTerm :: Term,
    raftStateLastCandidate :: Maybe Name,
    raftStateReady :: Bool,
    raftStateName :: Name,
    raftStateConfigurationIndex :: Maybe Index,
    raftStateConfiguration :: RaftConfiguration,
    raftStateMembers :: Members,
    raftStateData :: v
}

deriving instance (Eq v) => Eq (RaftState v)
deriving instance (Show v) => Show (RaftState v)

{-|
Create a fresh 'RaftState' instance.
-}
mkRaftState :: (Serialize v) => v -> RaftConfiguration -> Name -> RaftState v
mkRaftState initialData cfg name = RaftState {
    raftStateCurrentTerm = 0,
    raftStateLastCandidate = Nothing,
    raftStateReady = False,
    raftStateName = name,
    raftStateConfigurationIndex = Nothing,
    raftStateConfiguration = cfg,
    raftStateMembers = mkMembers cfg initialRaftTime,
    raftStateData = initialData
}

{-|
The type of entry that a 'RaftLog' manages.
-}
data RaftLogEntry e = (Serialize e) => RaftLogEntry {
    entryTerm :: Term,
    entryAction :: RaftAction e
}

deriving instance (Eq e) => Eq (RaftLogEntry e)
deriving instance (Show e) => Show (RaftLogEntry e)

instance (Serialize e) => Serialize (RaftLogEntry e) where
    get = do
        term <- get
        action <- get
        return $ RaftLogEntry term action
    put (RaftLogEntry term action) = do
        put term
        put action

instance (Serialize e,State v IO e) => State (RaftState v) IO (RaftLogEntry e) where
    canApplyEntry oldRaftState entry = do
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
                canApplyEntry oldData cmd
            -- TODO check configuration cases
            canApply _ = return True

    applyEntry oldRaftState entry = applyAction $ entryAction entry
        where
            applyAction (Cmd cmd) = do
                let oldData = raftStateData oldRaftState
                newData <- applyEntry oldData cmd
                return $ oldRaftState {raftStateData = newData}
            applyAction action = do
                let cfg = applyConfigurationAction (clusterConfiguration $ raftStateConfiguration oldRaftState) action
                infoM _log $ printf "New configuration is %v" (show cfg)
                return $ oldRaftState {
                    raftStateConfiguration = (raftStateConfiguration oldRaftState) {
                        clusterConfiguration = cfg
                    }
                }

{-|
The current 'Term' for this instance.
-}
raftCurrentTerm :: (RaftLog l e v) => RaftContext l e v -> Term
raftCurrentTerm raft = raftStateCurrentTerm $ raftState raft

{-|
The 'Name' this instance uses for communicating in the network.
-}
raftName :: (RaftLog l e v) => RaftContext l e v -> Name
raftName raft = raftStateName $ raftState raft

{-|
Update the current term in a new 'RaftContext'
-}
setRaftTerm :: Term -> RaftContext l e v -> RaftContext l e v
setRaftTerm term raft = raft {
        raftState = (raftState raft) {
            raftStateCurrentTerm = term
            }
        }

{-|
Update the current term in a new 'RaftContext'
-}
setRaftMembers :: Members -> RaftContext l e v -> RaftContext l e v
setRaftMembers members raft = raft {
        raftState = (raftState raft) {
            raftStateMembers = members
            }
        }

{-|
The current state of 'Members' in the cluster; only leaders track 'Member' state,
so in followers the valueof 'Members' is less useful.
-}
raftMembers :: (RaftLog l e v) => RaftContext l e v -> Members
raftMembers raft = raftStateMembers $ raftState raft

{-|
Computes 'membersSafeAppendedTerm' on this instance's 'Members'.
-}
raftSafeAppendedTerm :: (RaftLog l e v) => RaftContext l e v -> Term
raftSafeAppendedTerm raft = 
    let members = raftMembers raft
        cfg = raftConfiguration raft
        in membersSafeAppendedTerm members cfg

{-|
Update the readiness to handle client requests
-}
setRaftReady :: Bool -> RaftContext l e v -> RaftContext l e v
setRaftReady ready raft = raft {
                    raftState = (raftState raft) {
                        raftStateReady = ready
                        }
                    }
{-|
Returns 'True' if this instance is ready to serve clients.
-}
isRaftReady :: RaftContext l e v -> Bool
isRaftReady raft = raftStateReady $ raftState raft

{-|
Update the last candidate in a new 'RaftContext'
-}
setRaftLastCandidate :: Maybe Name -> RaftContext l e v -> RaftContext l e v
setRaftLastCandidate candidate raft = raft {
                    raftState = (raftState raft) {
                        raftStateLastCandidate = candidate
                        }
                    }

{-|
Update the 'RaftState' in a new 'RaftContext' to specify a new leader
-}
setRaftLeader :: Maybe Name -> RaftContext l e v -> RaftContext l e v
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
{-|
Returns 'True' if this instance is operating as the leader.
-}
isRaftLeader :: (RaftLog l e v) => RaftContext l e v -> Bool
isRaftLeader raft = (Just $ raftName raft) == (clusterLeader $ raftConfiguration raft)

{-|
Update the 'RaftLog' in this instance.
-}
setRaftLog :: (RaftLog l e v) => l -> RaftContext l e v -> RaftContext l e v
setRaftLog rlog raft = raft {
        raftLog = rlog
        }

{-|
Change the 'Configuration' in this instance.
-}
setRaftConfiguration :: (RaftLog l e v) => Configuration -> RaftContext l e v -> RaftContext l e v
setRaftConfiguration cfg raft =
    let newState = (raftState raft) {
        raftStateConfiguration = (raftStateConfiguration $ raftState raft) {
                            clusterConfiguration = cfg
        }}
        in setRaftState newState raft

{-|
Return the 'Configuration' for this instance.
-}
raftConfiguration :: (RaftLog l e v) => RaftContext l e v -> Configuration 
raftConfiguration raft = clusterConfiguration $ raftStateConfiguration $ raftState raft

{-|
Update the 'RaftState' for this instance.
-}
setRaftState :: (RaftLog l e v) => RaftState v -> RaftContext l e v -> RaftContext l e v
setRaftState state raft = raft {
        raftState = state
        }


--------------------------------------------------------------------------------
-- List log
--------------------------------------------------------------------------------

{-|
A simple implementation of a 'Log' and 'RaftLog' useful in many scenarios.  Since
typically there should not be that many uncommitted entries (e.g., appended
but not committed) in a log, then the size of this list should be small, relative
to the number of operations performed through it. As a 'ListLog' implements 'Serialize',
applications may choose to persist the log in its entirety to stable storage
as needed.
-}
data ListLog e v = (Serialize e,Serialize v) => ListLog {
    listLogLastCommitted :: RaftTime,
    listLogLastAppended :: RaftTime,
    listLogEntries :: [RaftLogEntry e]
}

deriving instance (Eq e) => Eq (ListLog e v)
deriving instance (Show e) => Show (ListLog e v)

instance (Serialize e,State v IO e) => Log (ListLog e v) IO (RaftLogEntry e) (RaftState v) where

    lastCommitted log = logIndex $ listLogLastCommitted log

    lastAppended log = logIndex $ listLogLastAppended log

    appendEntries log index newEntries = do
        if null newEntries
            then return log
            else do
                let term = maximum $ map entryTerm newEntries
                return log {
                    listLogLastAppended = RaftTime term (index + (length newEntries) - 1),
                    listLogEntries = (take (index + 1) (listLogEntries log)) ++ newEntries
                }
    fetchEntries log index count = do
        let entries = listLogEntries log
        return $ take count $ drop index entries

    commitEntry oldLog commitIndex entry = do
        let newLog = oldLog {
                listLogLastCommitted = RaftTime (entryTerm entry) commitIndex
                }
        return newLog

    checkpoint oldLog oldState = return (oldLog,oldState)

instance (Serialize e,Serialize v,State v IO e) => RaftLog (ListLog e v) e v where
    lastAppendedTime = listLogLastAppended
    lastCommittedTime = listLogLastCommitted

{-|
Create a new 'ListLog'.
-}
mkListLog :: (Serialize e,Serialize v) => IO (ListLog e v)
mkListLog = let initial = RaftTime (-1) (-1)
               in return $ ListLog initial initial []
