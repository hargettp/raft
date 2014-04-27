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
    applyConfigurationAction,
) where

-- local imports

import Control.Consensus.Raft.Types

-- external imports

import qualified Data.List as L
import Data.Serialize
import qualified Data.Set as S
import Data.Typeable

import GHC.Generics

import Network.Endpoints

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
          configurationLeader :: Maybe Name,
          configurationParticipants :: S.Set Name,
          configurationTimeouts :: Timeouts
          }
          | JointConfiguration {
          jointOldConfiguration :: Configuration,
          jointNewConfiguration :: Configuration
          } deriving (Generic,Show,Typeable,Eq)

instance Serialize Configuration

newConfiguration :: [Name] -> Configuration
newConfiguration participants = Configuration {
    configurationLeader = Nothing,
    configurationParticipants = S.fromList participants,
    configurationTimeouts = defaultTimeouts
}

clusterLeader :: Configuration -> Maybe Name
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

clusterMembers :: Configuration -> [Name]
clusterMembers (Configuration _ participants _) = S.toList participants
clusterMembers (JointConfiguration jointOld jointNew) = S.toList $ S.fromList $ clusterMembers jointOld ++ (clusterMembers jointNew)

clusterMembersOnly :: Configuration -> [Name]
clusterMembersOnly cfg = case clusterLeader cfg of
    Just ldr -> L.delete ldr (clusterMembers cfg)
    Nothing -> clusterMembers cfg

addClusterParticipants :: Configuration -> [Name] -> Configuration
addClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.union (configurationParticipants cfg) $ S.fromList participants
    }
addClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterParticipants jointNew participants
    }

removeClusterParticipants :: Configuration -> [Name] -> Configuration
removeClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.difference (configurationParticipants cfg) $ S.fromList participants
    }
removeClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterParticipants jointNew participants
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
applyConfigurationAction initial _ = initial
