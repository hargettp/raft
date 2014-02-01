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

-- external imports

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Server a = Server {
    serverId :: ServerId,
    serverConfiguration :: Configuration
}