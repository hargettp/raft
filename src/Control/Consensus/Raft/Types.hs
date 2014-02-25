{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StandaloneDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Types
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

module Control.Consensus.Raft.Types (
    -- * General types
    ServerId,
    Term,
    Timeout,
    -- * Raft state
    RaftState(..),
    RaftServer,
    RaftLog,
    RaftLogEntry(..),
    Server(..),
    ServerState(..),
    changeRaftTerm,
    changeRaftLeader,
    changeRaftLastCandidate,
    changeRaftLog,
    -- * Actions
    Action(..),
    Command,
    -- * Configuration
    Configuration(..),
    newConfiguration,
    clusterLeader,
    clusterMembers,
    clusterMembersOnly,
    addClusterParticipants,
    removeClusterParticipants,
    addClusterObservers,
    removeClusterObservers,
    applyConfigurationAction
) where

-- local imports

-- external imports

import qualified Data.ByteString as B
import qualified Data.List as L
import Control.Consensus.Log
import Data.Serialize
import qualified Data.Set as S
import Data.Typeable

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type Term = Int

type ServerId = Name

{-|
Type used for timeouts.  Mostly used for code clarity.
-}
type Timeout = Int

{- |
A configuration identifies all the members of a cluster and the nature of their participation 
in the cluster.
-}
data Configuration = Configuration {
          configurationLeader :: Maybe ServerId,
          configurationParticipants :: S.Set ServerId,
          configurationObservers :: S.Set ServerId
          }
          | JointConfiguration {
          jointOldConfiguration :: Configuration,
          jointNewConfiguration :: Configuration
          } deriving (Generic,Show,Typeable,Eq)

instance Serialize Configuration

newConfiguration :: [ServerId] -> Configuration
newConfiguration participants = Configuration {
    configurationLeader = Nothing,
    configurationParticipants = S.fromList participants,
    configurationObservers = S.empty
}

clusterLeader :: Configuration -> Maybe ServerId
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

clusterMembers :: Configuration -> [ServerId]
clusterMembers (Configuration _ participants observers) = S.toList $ S.union participants observers
clusterMembers (JointConfiguration jointOld jointNew) = S.toList $ S.fromList $ clusterMembers jointOld ++ (clusterMembers jointNew)

clusterMembersOnly :: Configuration -> [ServerId]
clusterMembersOnly cfg = case clusterLeader cfg of
    Just ldr -> L.delete ldr (clusterMembers cfg)
    Nothing -> clusterMembers cfg

addClusterParticipants :: Configuration -> [ServerId] -> Configuration
addClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.union (configurationParticipants cfg) $ S.fromList participants
    }
addClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterParticipants jointNew participants
    }

removeClusterParticipants :: Configuration -> [ServerId] -> Configuration
removeClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.difference (configurationParticipants cfg) $ S.fromList participants
    }
removeClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterParticipants jointNew participants
    }

addClusterObservers :: Configuration -> [ServerId] -> Configuration
addClusterObservers cfg@(Configuration _ _ _) observers = cfg {
    configurationObservers = S.union (configurationObservers cfg) $ S.fromList observers
    }
addClusterObservers (JointConfiguration jointOld jointNew) observers = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterObservers jointNew observers
    }

removeClusterObservers :: Configuration -> [ServerId] -> Configuration
removeClusterObservers cfg@(Configuration _ _ _) observers = cfg {
    configurationObservers = S.union (configurationObservers cfg) $ S.fromList observers
    }
removeClusterObservers (JointConfiguration jointOld jointNew) observers = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterObservers jointNew observers
    }

{-|
Apply the 'Action' to the 'Configuration', if it is a configuration change; otherwise,
leave the configuration unchanged
-}
applyConfigurationAction :: Configuration -> Action -> Configuration
applyConfigurationAction initial (AddParticipants participants) = addClusterParticipants initial participants
applyConfigurationAction initial (RemoveParticipants participants) = removeClusterParticipants initial participants
applyConfigurationAction initial (AddObservers observers) = addClusterObservers initial observers
applyConfigurationAction initial (RemoveObservers observers) = removeClusterObservers initial observers
applyConfigurationAction initial _ = initial

{-|
A minimal 'Log' sufficient for a 'Server' to particpate in the Raft algorithm'.
-}
class (LogIO l RaftLogEntry (ServerState v)) => RaftLog l v

{-|
A minimal 'Server' capable of participating in the Raft algorithm.
-}
type RaftServer l v = Server l RaftLogEntry (ServerState v)

{-|
Encapsulates the state necessary for the Raft algorithm, depending
on a 'RaftServer' for customizing the use of the algorithm to a 
specific application.
-}
data RaftState l v = (RaftLog l v) => RaftState {
    raftCurrentTerm :: Term,
    raftLastCandidate :: Maybe ServerId,
    raftServer :: RaftServer l v
}

{-|
Update the current term in a new 'RaftState'
-}
changeRaftTerm :: Term -> RaftState l v -> RaftState l v
changeRaftTerm term raft = raft {
    raftCurrentTerm = term
}

{-|
Update the last candidate in a new 'RaftState'
-}
changeRaftLastCandidate :: Maybe ServerId -> RaftState l v -> RaftState l v
changeRaftLastCandidate candidate raft = raft {
    raftLastCandidate = candidate
}

{-|
Update the 'ServerState' in a new 'RaftState' to specify a new leader
-}
changeRaftLeader :: Maybe ServerId -> RaftState l v -> RaftState l v
changeRaftLeader leader raft = raft {
    raftServer = (raftServer raft) {
        serverState = (serverState $ raftServer raft) {
            serverConfiguration = (serverConfiguration $ serverState $ raftServer raft) {
                configurationLeader = leader
            }}}
}

changeRaftLog :: (RaftLog l v) => l -> RaftState l v -> RaftState l v
changeRaftLog rlog raft = raft {
    raftServer = (raftServer raft) {
            serverLog = rlog
        }
    }

data RaftLogEntry =  RaftLogEntry {
    entryTerm :: Term,
    entryAction :: Action
} deriving (Eq,Show,Generic)

instance Serialize RaftLogEntry

type Command = B.ByteString

data Action = AddParticipants [ServerId]
    | RemoveParticipants [ServerId]
    | AddObservers [ServerId]
    | RemoveObservers [ServerId]
    | Cmd Command
    deriving (Eq,Show,Generic)

instance Serialize Action

data Server l e v = (LogIO l e v) => Server {
    serverId :: ServerId,
    serverLog :: l,
    serverState :: v
}

data ServerState v = (Eq v,Show v) => ServerState {
    serverConfiguration :: Configuration,
    serverData :: v
}

deriving instance Eq (ServerState v)
deriving instance Show (ServerState v)