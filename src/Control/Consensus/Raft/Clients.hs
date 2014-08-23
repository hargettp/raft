{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Clients
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

module Control.Consensus.Raft.Clients (
    Clients,
    mkClients,
    isClientRequestAppended,
    appendClientRequest,
    isClientRequestCommitted,
    commitClientRequest
) where

-- local imports

-- external imports

import Data.Log

import qualified Data.Map as M

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data ClientLog = ClientLog {
    clientAppendedIndex :: Index,
    clientCommittedIndex :: Index
} deriving (Show,Eq)

type Clients = M.Map Name ClientLog

mkClients :: Clients
mkClients = M.empty

isClientRequestAppended :: Name -> Index -> Clients -> Bool
isClientRequestAppended name index clients = case (M.lookup name clients)
    of Just client -> index <= clientAppendedIndex client
       Nothing -> False

appendClientRequest :: Name -> Index -> Clients -> Clients
appendClientRequest name index clients =
    case (M.lookup name clients) of
        Just client -> let oldIndex = clientAppendedIndex client
                           in M.insert name (client {clientAppendedIndex = max index oldIndex}) clients
        Nothing -> M.insert name (ClientLog index  (-1 :: Index)) clients

isClientRequestCommitted :: Name -> Index -> Clients -> Bool
isClientRequestCommitted name index clients = case (M.lookup name clients)
    of Just client -> index <= clientCommittedIndex client
       Nothing -> False

commitClientRequest :: Name -> Index -> Clients -> Clients
commitClientRequest name index clients = 
    let newClients = appendClientRequest name index clients
    in case (M.lookup name newClients) of 
        Just client -> let oldIndex = clientCommittedIndex client
                           in M.insert name (client {clientCommittedIndex = max index oldIndex}) clients
        -- we are unlikely to arrive at this case, as appendClientRequest probably
        -- already gave us an entry in the map--but if we did, we are appended at least
        -- as far as we've committed
        Nothing -> M.insert name (ClientLog index index) newClients
