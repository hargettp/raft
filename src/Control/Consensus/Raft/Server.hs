{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ExistentialQuantification #-}

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

data Server l e v = (LogIO l e v) => Server {
    serverId :: ServerId,
    serverConfiguration :: Configuration,
    serverLog :: l,
    serverState :: v
}