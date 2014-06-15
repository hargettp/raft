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
    RaftConfiguration(..),
    mkRaftConfiguration,

    module Control.Consensus.Configuration
) where

-- local imports

import Control.Consensus.Raft.Types

-- external imports

import Control.Consensus.Configuration

import Data.Serialize

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
data RaftConfiguration = RaftConfiguration {
    clusterConfiguration :: Configuration,
    clusterTimeouts :: Timeouts
    } deriving (Generic,Show,Eq)

instance Serialize RaftConfiguration

mkRaftConfiguration :: [Name] -> RaftConfiguration
mkRaftConfiguration participants = RaftConfiguration {
    clusterConfiguration = mkConfiguration participants,
    clusterTimeouts = defaultTimeouts
}
