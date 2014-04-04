{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Configuration
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

module Control.Consensus.Raft.Configuration (
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
    applyConfigurationAction,
    -- * Actions
    Action(..),
    Command    
) where

-- local imports

import Control.Consensus.Raft.Types

-- external imports

import qualified Data.ByteString as B
import qualified Data.List as L
import Data.Serialize
import qualified Data.Set as S
import Data.Typeable

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Cofiguration
--------------------------------------------------------------------------------

{- |
A configuration identifies all the members of a cluster and the nature of their participation 
in the cluster.
-}
data Configuration = Configuration {
          configurationLeader :: Maybe ServerId,
          configurationParticipants :: S.Set ServerId,
          configurationObservers :: S.Set ServerId,
          configurationTimeouts :: Timeouts
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
    configurationObservers = S.empty,
    configurationTimeouts = defaultTimeouts
}

clusterLeader :: Configuration -> Maybe ServerId
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

clusterMembers :: Configuration -> [ServerId]
clusterMembers (Configuration _ participants observers _) = S.toList $ S.union participants observers
clusterMembers (JointConfiguration jointOld jointNew) = S.toList $ S.fromList $ clusterMembers jointOld ++ (clusterMembers jointNew)

clusterMembersOnly :: Configuration -> [ServerId]
clusterMembersOnly cfg = case clusterLeader cfg of
    Just ldr -> L.delete ldr (clusterMembers cfg)
    Nothing -> clusterMembers cfg

addClusterParticipants :: Configuration -> [ServerId] -> Configuration
addClusterParticipants cfg@(Configuration _ _ _ _) participants = cfg {
    configurationParticipants = S.union (configurationParticipants cfg) $ S.fromList participants
    }
addClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterParticipants jointNew participants
    }

removeClusterParticipants :: Configuration -> [ServerId] -> Configuration
removeClusterParticipants cfg@(Configuration _ _ _ _) participants = cfg {
    configurationParticipants = S.difference (configurationParticipants cfg) $ S.fromList participants
    }
removeClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterParticipants jointNew participants
    }

addClusterObservers :: Configuration -> [ServerId] -> Configuration
addClusterObservers cfg@(Configuration _ _ _ _) observers = cfg {
    configurationObservers = S.union (configurationObservers cfg) $ S.fromList observers
    }
addClusterObservers (JointConfiguration jointOld jointNew) observers = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterObservers jointNew observers
    }

removeClusterObservers :: Configuration -> [ServerId] -> Configuration
removeClusterObservers cfg@(Configuration _ _ _ _) observers = cfg {
    configurationObservers = S.union (configurationObservers cfg) $ S.fromList observers
    }
removeClusterObservers (JointConfiguration jointOld jointNew) observers = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterObservers jointNew observers
    }

--------------------------------------------------------------------------------
-- Actions
--------------------------------------------------------------------------------

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

type Command = B.ByteString

data Action = AddParticipants [ServerId]
    | RemoveParticipants [ServerId]
    | AddObservers [ServerId]
    | RemoveObservers [ServerId]
    | Cmd Command
    deriving (Eq,Show,Generic)

instance Serialize Action