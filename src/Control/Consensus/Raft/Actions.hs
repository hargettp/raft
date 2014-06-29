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
-- (..... module description .....)
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft.Actions (
    Command,
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

data ConfigurationCommand = AddParticipants [Name]
    | RemoveParticipants [Name]
    | SetConfiguration Configuration
    deriving (Eq,Show,Generic)

instance Serialize ConfigurationCommand

data RaftAction c = (Command c) => Cfg ConfigurationCommand
    | Cmd c

deriving instance (Command c) => Eq (RaftAction c)
deriving instance (Command c) => Show (RaftAction c)

instance (Command c) => Serialize (RaftAction c) where
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

isCommandAction :: (Command c) => RaftAction c -> Bool
isCommandAction (Cmd _) = True
isCommandAction _ = False

isConfigurationAction :: (Command c) => RaftAction c -> Bool
isConfigurationAction = not . isCommandAction

{-|
Commands are the specific operations applied to 'Control.Consensus.Log.State's
to transform them into a new 'Control.Consensus.Log.State'.
-}
class (Eq c,Show c,Serialize c) => Command c

--------------------------------------------------------------------------------
-- Configuration actions
--------------------------------------------------------------------------------

{-|
Apply the 'Action' to the 'Configuration', if it is a configuration change; otherwise,
leave the configuration unchanged
-}
applyConfigurationAction :: (Command c) => Configuration -> RaftAction c -> Configuration
applyConfigurationAction cfg (Cfg cmd) = applyConfigurationCommand cfg cmd
applyConfigurationAction cfg (Cmd _) = cfg

applyConfigurationCommand :: Configuration -> ConfigurationCommand -> Configuration
applyConfigurationCommand _ (SetConfiguration cfg) = cfg
applyConfigurationCommand (JointConfiguration jointOld jointNew) cmd = JointConfiguration jointOld (applyConfigurationCommand jointNew cmd)
applyConfigurationCommand initial (AddParticipants participants) = addClusterParticipants initial participants
applyConfigurationCommand initial (RemoveParticipants participants) = removeClusterParticipants initial participants