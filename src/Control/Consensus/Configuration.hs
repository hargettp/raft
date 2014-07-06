{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Configuration
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- A 'Configuration' generally describes membership in a cluster. Within a cluster, there
-- may or may not be an active leader at any given time, there should be 1 or more participants
-- actively engaged in consensus algorithms and the work of the cluster, and there may be
-- 0 or more observers: these are passive members who receive updates on shared cluster
-- state, but do not participate in consensus decisions or the work of the cluster.
--
-----------------------------------------------------------------------------

module Control.Consensus.Configuration (
    -- * Configuration
    Configuration(..),
    mkConfiguration,
    isJointConfiguration,
    clusterLeader,
    isClusterParticipant,
    isClusterMember,
    clusterMembers,
    clusterMembersOnly,
    clusterParticipants,
    addClusterParticipants,
    removeClusterParticipants,
    setClusterParticipants
) where

-- local imports

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
data Configuration =
  -- | A simple configuration with an optional leader, some participants, and some observers.
  Configuration {
          configurationLeader :: Maybe Name,
          configurationParticipants :: S.Set Name,
          configurationObservers :: S.Set Name
          }
  -- | A combined configuration temporarily used for transitioning between two simplie configurations.
  | JointConfiguration {
          jointOldConfiguration :: Configuration,
          jointNewConfiguration :: Configuration
          } deriving (Generic,Show,Typeable,Eq)

instance Serialize Configuration

{-|
Create a new configuration with no leader and no observers; the supplie names
will be recorded as participants.
-}
mkConfiguration :: [Name] -> Configuration
mkConfiguration participants = Configuration {
    configurationLeader = Nothing,
    configurationParticipants = S.fromList participants,
    configurationObservers = S.empty
}

{-|
Returns 'True' if the configuration is a joint one.
-}
isJointConfiguration :: Configuration -> Bool
isJointConfiguration (Configuration _ _ _) = False
isJointConfiguration (JointConfiguration _ _) = True

{-|
Returns the leader of the configuration, if any.
-}
clusterLeader :: Configuration -> Maybe Name
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

{-|
Returns 'True' if the supplied name is a participant in the configuration.
-}
isClusterParticipant :: Name -> Configuration -> Bool
isClusterParticipant name (Configuration _ participants _) = S.member name participants
isClusterParticipant name (JointConfiguration jointOld jointNew) = 
    (isClusterParticipant name jointOld) || (isClusterParticipant name jointNew)

{-|
Return all members of the cluster (e.g., participants and observers) as a list.
-}
clusterMembers :: Configuration -> [Name]
clusterMembers (Configuration _ participants observers) = S.toList $ S.union participants observers
clusterMembers (JointConfiguration jointOld jointNew) =
    S.toList $ S.union (S.fromList $ clusterMembers jointOld) (S.fromList $ clusterMembers jointNew)

{-|
Return true if the supplied name is a participant or observer in the cluster.
-}
isClusterMember :: Name -> Configuration -> Bool
isClusterMember name (Configuration _ participants observers) = S.member name participants || S.member name observers
isClusterMember name (JointConfiguration jointOld jointNew) = 
    (isClusterMember name jointOld) || (isClusterMember name jointNew)

{-|
Return just the participants in the cluster as a list.
-}
clusterParticipants :: Configuration -> [Name]
clusterParticipants (Configuration _ participants _) = (S.toList participants)
clusterParticipants (JointConfiguration jointOld jointNew) = 
    S.toList $ S.fromList $ clusterParticipants jointOld ++ (clusterParticipants jointNew)

{-|
Return members of the cluster, excluding any leader
-}
clusterMembersOnly :: Configuration -> [Name]
clusterMembersOnly cfg = case clusterLeader cfg of
    Just ldr -> L.delete ldr (clusterMembers cfg)
    Nothing -> clusterMembers cfg

{-|
Return a new configuration with the supplied participants added to the cluster.
-}
addClusterParticipants :: Configuration -> [Name] -> Configuration
addClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.union (configurationParticipants cfg) $ S.fromList participants
    }
addClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = addClusterParticipants jointNew participants
    }

{-|
Return a new configuration with the participants removed from the cluster.
-}
removeClusterParticipants :: Configuration -> [Name] -> Configuration
removeClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.difference (configurationParticipants cfg) $ S.fromList participants
    }
removeClusterParticipants (JointConfiguration jointOld jointNew) participants = JointConfiguration {
    jointOldConfiguration = jointOld,
    jointNewConfiguration = removeClusterParticipants jointNew participants
    }

{-|
Return a new configuration with the provided list of participants replacing any prior participants
in the cluster.
-}
setClusterParticipants :: Configuration -> [Name] -> Configuration
setClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.fromList participants
    }
setClusterParticipants (JointConfiguration _ jointNew) participants = setClusterParticipants jointNew participants
