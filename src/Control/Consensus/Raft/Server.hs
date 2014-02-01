{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Server
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

module Control.Consensus.Raft.Server (
    Server(..)
) where

-- local imports

import Control.Consensus.Raft.Types

import Data.Log

-- external imports

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

class (Log l m e v) => Server s l m e v | s -> l, l -> m , l -> e , l -> v where
    -- | Create a new server with an initial value, configuration, and log
    newServer :: v -> Configuration -> l -> m s
    -- | Return the server's unique identifier
    serverId :: s -> ServerId
    -- | Return the server's current configuration
    serverConfiguration :: m Configuration
    -- | Restore the server to its last checkpoint, or to its
    -- initial state if never checkpointed
    restoreServer :: s -> m ()
    -- | Append zero or more entries to the server's log, starting
    -- at the indicated 'Index'.
    appendLog :: s -> [e v] -> Index -> m ()
    -- | Read @count@ entries from the server's log, starting at the
    -- provided 'Index'
    readLog :: s -> Index -> Int -> m [e v]
    -- | Commit all entries up to and including the indicated 'Index'
    commitServer :: s -> Index -> m ()
    -- | Preserve the server's state (e.g., log, configuration, and value)
    -- such that if a 'restoreServer' is later invoked the server
    -- will resume using the same log, configuration and value.
    checkpointServer :: s -> m ()
