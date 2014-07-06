{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
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
-- An action is an operation that can be applied to a 'Control.Consensus.Raft.Log.RaftState'
-- to change the state to a new one. As implemented here, a 'RaftAction' contains either
-- a 'ConfigurationCommand' that alters the configuration of the cluster or an ordinary
-- application-specific command that changes the 'Data.Log.State' behind the state machine.
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

-- external imports

import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
A command that changes the 'Configuration' of a cluster.
-}
data ConfigurationCommand = AddParticipants [Name]
    | RemoveParticipants [Name]
    | SetConfiguration Configuration
    deriving (Eq,Show,Generic)

instance Serialize ConfigurationCommand

{-|
An action that either changes the configuration or the state of a state machine.
-}
data RaftAction c = (Serialize c) => Cfg ConfigurationCommand
    | Cmd c

deriving instance (Eq c) => Eq (RaftAction c)
deriving instance (Show c) => Show (RaftAction c)

instance (Serialize c) => Serialize (RaftAction c) where
    get = do
        kind <- getWord8
        case kind of
            0 -> do
                cfg <- get
                return $ Cfg cfg
            _ -> do
                cmd <- get
                return $ Cmd cmd
    put (Cfg cfg) = do
        putWord8 0
        put cfg
    put (Cmd cmd) = do
        putWord8 1
        put cmd

{-|
Return 'True' if the action is a command for changing state.
-}
isCommandAction :: (Serialize c) => RaftAction c -> Bool
isCommandAction (Cmd _) = True
isCommandAction _ = False

{-|
Return 'True' if the action is a command for changing configuration.
-}
isConfigurationAction :: (Serialize c) => RaftAction c -> Bool
isConfigurationAction = not . isCommandAction

--------------------------------------------------------------------------------
-- Configuration actions
--------------------------------------------------------------------------------

{-|
Apply the 'Action' to the 'Configuration', if it is a configuration change; otherwise,
leave the configuration unchanged
-}
applyConfigurationAction :: (Serialize c) => Configuration -> RaftAction c -> Configuration
applyConfigurationAction cfg (Cfg cmd) = applyConfigurationCommand cfg cmd
applyConfigurationAction cfg (Cmd _) = cfg

applyConfigurationCommand :: Configuration -> ConfigurationCommand -> Configuration
applyConfigurationCommand _ (SetConfiguration cfg) = cfg
applyConfigurationCommand (JointConfiguration jointOld jointNew) cmd = JointConfiguration jointOld (applyConfigurationCommand jointNew cmd)
applyConfigurationCommand initial (AddParticipants participants) = addClusterParticipants initial participants
applyConfigurationCommand initial (RemoveParticipants participants) = removeClusterParticipants initial participants
