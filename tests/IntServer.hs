{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  IntServer
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- Basic log implementation for simple arithmetic on Ints, useful
-- for unit tests.
--
-----------------------------------------------------------------------------

module IntServer (
    IntCommand(..),
    IntRaft,
    IntLogEntry(..),
    IntLog,
    IntState(..),
    mkIntLog
) where

-- local imports

import Control.Consensus.Raft

-- external imports

import Prelude hiding (log)

import Data.Serialize

import GHC.Generics

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data IntCommand = Add Int
    | Subtract Int
    | Multiply Int
    | Divide Int
    deriving (Eq,Show,Generic)

instance Serialize IntCommand

data IntState = IntState Int deriving (Eq,Show,Generic,Ord)

instance Serialize IntState

applyIntCommand :: IntState -> IntCommand -> IO IntState
applyIntCommand (IntState initial) (Add value) = return $ IntState $ initial + value
applyIntCommand (IntState initial) (Subtract value) = return $ IntState $ initial - value
applyIntCommand (IntState initial) (Multiply value) = return $ IntState $ initial  * value
applyIntCommand (IntState initial) (Divide value) = return $ IntState $ initial `quot` value

data IntLogEntry = IntLogEntry {
    entryCommand :: IntCommand
} deriving (Eq,Show,Generic)

instance Serialize IntLogEntry

type IntLog = ListLog IntCommand IntState

mkIntLog :: IO IntLog
mkIntLog = mkListLog

instance State IntState IO IntCommand where

    canApplyEntry _ _ = return True

    applyEntry = applyIntCommand

type IntRaft = Raft IntLog IntCommand IntState
