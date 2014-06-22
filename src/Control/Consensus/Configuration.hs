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
-- (..... module description .....)
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
data Configuration = Configuration {
          configurationLeader :: Maybe Name,
          configurationParticipants :: S.Set Name,
          configurationObservers :: S.Set Name
          }
          | JointConfiguration {
          jointOldConfiguration :: Configuration,
          jointNewConfiguration :: Configuration
          } deriving (Generic,Show,Typeable,Eq)

instance Serialize Configuration

mkConfiguration :: [Name] -> Configuration
mkConfiguration participants = Configuration {
    configurationLeader = Nothing,
    configurationParticipants = S.fromList participants,
    configurationObservers = S.empty
}

isJointConfiguration :: Configuration -> Bool
isJointConfiguration (Configuration _ _ _) = False
isJointConfiguration (JointConfiguration _ _) = True

clusterLeader :: Configuration -> Maybe Name
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

isClusterParticipant :: Name -> Configuration -> Bool
isClusterParticipant name (Configuration _ participants _) = S.member name participants
isClusterParticipant name (JointConfiguration jointOld jointNew) = 
    (isClusterParticipant name jointOld) || (isClusterParticipant name jointNew)

clusterMembers :: Configuration -> [Name]
clusterMembers (Configuration _ participants observers) = S.toList $ S.union participants observers
clusterMembers (JointConfiguration jointOld jointNew) =
    S.toList $ S.union (S.fromList $ clusterMembers jointOld) (S.fromList $ clusterMembers jointNew)

isClusterMember :: Name -> Configuration -> Bool
isClusterMember name (Configuration _ participants observers) = S.member name participants || S.member name observers
isClusterMember name (JointConfiguration jointOld jointNew) = 
    (isClusterMember name jointOld) || (isClusterMember name jointNew)

clusterParticipants :: Configuration -> [Name]
clusterParticipants (Configuration _ participants _) = (S.toList participants)
clusterParticipants (JointConfiguration jointOld jointNew) = 
    S.toList $ S.fromList $ clusterParticipants jointOld ++ (clusterParticipants jointNew)

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

setClusterParticipants :: Configuration -> [Name] -> Configuration
setClusterParticipants cfg@(Configuration _ _ _) participants = cfg {
    configurationParticipants = S.fromList participants
    }
setClusterParticipants (JointConfiguration _ jointNew) participants = setClusterParticipants jointNew participants
