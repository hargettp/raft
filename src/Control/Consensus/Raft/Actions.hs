{-# LANGUAGE DeriveGeneric #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Actions
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
    Action(..),
    Command,
    isCommandAction,
    isConfigurationAction,
    -- * Configuration actions
    applyConfigurationAction
) where

-- local imports

import Control.Consensus.Raft.Types

-- external imports

import qualified Data.ByteString as B
import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


data Action = AddParticipants [Name]
    | RemoveParticipants [Name]
    | SetConfiguration Configuration
    | Cmd Command
    deriving (Eq,Show,Generic)

instance Serialize Action

{-|
Commands are the specific operations applied to 'Control.Consensus.Log.State's
to transform them into a new 'Control.Consensus.Log.State'. They are represented
here in their completely typeless form as a 'B.ByteString', because that's the
most concrete description of them.
-}
type Command = B.ByteString

isCommandAction :: Action -> Bool
isCommandAction (Cmd _) = True
isCommandAction _ = False

isConfigurationAction :: Action -> Bool
isConfigurationAction = not . isCommandAction

--------------------------------------------------------------------------------
-- Configuration actions
--------------------------------------------------------------------------------

{-|
Apply the 'Action' to the 'Configuration', if it is a configuration change; otherwise,
leave the configuration unchanged
-}
applyConfigurationAction :: Configuration -> Action -> Configuration
-- Common
applyConfigurationAction _ (SetConfiguration cfg) = cfg
-- Joint config
applyConfigurationAction (JointConfiguration jointOld jointNew) (AddParticipants participants) = JointConfiguration jointOld $ addClusterParticipants jointNew participants
applyConfigurationAction (JointConfiguration jointOld jointNew) (RemoveParticipants participants) = JointConfiguration jointOld $ removeClusterParticipants jointNew participants
applyConfigurationAction (JointConfiguration jointOld jointNew) _ = JointConfiguration jointOld jointNew
-- Single config
applyConfigurationAction initial (AddParticipants participants) = JointConfiguration initial $ addClusterParticipants initial participants
applyConfigurationAction initial (RemoveParticipants participants) = JointConfiguration initial $ removeClusterParticipants initial participants
applyConfigurationAction initial _ = initial
