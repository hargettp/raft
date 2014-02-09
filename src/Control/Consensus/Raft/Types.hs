{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}

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
    Configuration(..),
        clusterLeader,
        clusterMembers,
        clusterMembersOnly,
    ServerId,
    Term,
    Timeout
) where

-- local imports

-- external imports

import qualified Data.List as L
import Data.Serialize
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
          configurationParticipants :: [ServerId],
          configurationObservers :: [ServerId]
          } 
          | JointConfiguration {
          jointOldConfiguration :: Configuration,
          jointNewConfiguration :: Configuration
          } deriving (Generic,Show,Typeable,Eq)

instance Serialize Configuration

clusterLeader :: Configuration -> Maybe ServerId
clusterLeader Configuration {configurationLeader = leaderId} = leaderId
clusterLeader (JointConfiguration _ configuration) = clusterLeader configuration

clusterMembers :: Configuration -> [ServerId]
clusterMembers (Configuration _ participants observers) = participants ++ observers
clusterMembers (JointConfiguration jointOld jointNew) = (clusterMembers jointOld) ++ (clusterMembers jointNew)

clusterMembersOnly :: Configuration -> [ServerId]
clusterMembersOnly cfg = case clusterLeader cfg of
    Just ldr -> L.delete ldr (clusterMembers cfg)
    Nothing -> clusterMembers cfg
