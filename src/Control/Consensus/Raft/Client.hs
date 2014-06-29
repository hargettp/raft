-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Client
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

module Control.Consensus.Raft.Client (

    Client,
    newClient,

    performAction

) where

-- local imports

import Control.Consensus.Raft.Actions
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Protocol
import Control.Consensus.Raft.Types

-- external imports

import Control.Concurrent

import Network.Endpoints
import Network.RPC

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.client"

data Client = Client {
    clientEndpoint :: Endpoint,
    clientName :: Name,
    clientConfiguration :: RaftConfiguration
}

newClient :: Endpoint -> Name -> RaftConfiguration -> Client
newClient endpoint name cfg = Client {
    clientConfiguration = cfg,
    clientEndpoint = endpoint,
    clientName = name
}

{-|
Perform an 'Action' in the cluster.
-}
performAction :: (Command c) => Client -> RaftAction c -> IO RaftTime
performAction client action = do
    -- TODO consider whether there is an eventual timeout
    -- in case the cluster can't be reached
    let cfg = clientConfiguration client
        leader = case clusterLeader $ clusterConfiguration cfg of
            Just lead -> [lead]
            Nothing -> []
        members = leader ++ (clusterMembers $ clusterConfiguration cfg)
        cs = (newCallSite (clientEndpoint client) (clientName client))
    perform cs cfg members members
    where
        perform cs cfg members [] = do
            infoM _log $ "Client " ++ (clientName client) ++ " can't find any members"
            -- timeout in case there are issues
            threadDelay $ 100 * 1000
            infoM _log $ "Client " ++ (clientName client) ++ " searching again for members"
            perform cs cfg members members
        perform cs cfg members (leader:others) = do
            infoM _log $ "Client " ++ (clientName client) ++ " sending action " ++ (show action) ++ " to " ++ leader
            maybeResult <- goPerformAction cs cfg leader action
            infoM _log $ "Client " ++ (clientName client) ++ " sent action " ++ (show action) ++ " to " ++ leader
            infoM _log $ "Client " ++ (clientName client) ++ " received response " ++ (show maybeResult)
            case maybeResult of
                Just result -> if (memberActionSuccess result)
                    then return $ memberLastCommitted result
                    else case memberLeader result of
                        -- follow the redirect to the correct leader
                        Just newLeader -> perform cs cfg members (newLeader:others)
                        -- keep trying the others until a leader is found
                        Nothing -> perform cs cfg members others
                Nothing ->  perform cs cfg members others
