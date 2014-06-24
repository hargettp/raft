{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.RaftActions
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

module Control.Consensus.Raft.Actions (
    -- * Actions
    RaftAction(..),
    ConfigurationCommand(..),
    isCommandAction,
    isConfigurationAction,
    -- * Configuration actions
    applyConfigurationAction
) where

-- local imports

import Control.Consensus.Raft.Types

import Data.Log

-- external imports

import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data ConfigurationCommand = AddParticipants [Name]
    | RemoveParticipants [Name]
    | SetConfiguration Configuration
    deriving (Eq,Show,Generic)

instance Serialize ConfigurationCommand

data RaftAction = Cfg ConfigurationCommand
    | Cmd Command
    deriving (Generic)

deriving instance Eq RaftAction
deriving instance Show RaftAction

instance Serialize RaftAction

isCommandAction :: RaftAction -> Bool
isCommandAction (Cmd _) = True
isCommandAction _ = False

isConfigurationAction :: RaftAction -> Bool
isConfigurationAction = not . isCommandAction

--------------------------------------------------------------------------------
-- Configuration actions
--------------------------------------------------------------------------------

{-|
Apply the 'Action' to the 'Configuration', if it is a configuration change; otherwise,
leave the configuration unchanged
-}
applyConfigurationAction :: Configuration -> RaftAction -> Configuration
applyConfigurationAction cfg (Cfg cmd) = applyConfigurationCommand cfg cmd
applyConfigurationAction cfg (Cmd _) = cfg

applyConfigurationCommand :: Configuration -> ConfigurationCommand -> Configuration
applyConfigurationCommand _ (SetConfiguration cfg) = cfg
applyConfigurationCommand (JointConfiguration jointOld jointNew) cmd = JointConfiguration jointOld (applyConfigurationCommand jointNew cmd)
applyConfigurationCommand initial (AddParticipants participants) = addClusterParticipants initial participants
applyConfigurationCommand initial (RemoveParticipants participants) = removeClusterParticipants initial participants