{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Protocol
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

module Control.Consensus.Raft.Protocol (

    -- * Basic message types
    AppendEntries(..),

    RequestVote(..),

    -- * Client call
    goPerformAction,

    -- * Leader call
    goAppendEntries,

    -- * Member calls
    goRequestVote,

    -- * Member handlers
    onPerformAction,
    onAppendEntries,
    onRequestVote

) where

-- local imports

import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Types

-- external imports

import qualified Data.Map as M
import Data.Serialize

import GHC.Generics

import Network.Endpoints
import Network.RPC

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.protocol"

data AppendEntries =  AppendEntries {
    aeLeader :: Name,
    aeLeaderTerm :: Term,
    aePreviousTime :: RaftTime,
    aeCommittedTime :: RaftTime,
    aeEntries :: [RaftLogEntry]
} deriving (Eq,Show,Generic)

instance Serialize AppendEntries

data RequestVote = RequestVote {
        rvCandidate :: Name,
        rvCandidateTerm :: Term,
        rvCandidateLastEntryTime :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize RequestVote

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

goAppendEntries :: CallSite
            -> Configuration            -- ^^ Cluster configuration
            -> Term                     -- ^^ Leader's current term
            -> RaftTime                 -- ^^ `RaftTime` of entry just prior to the entries being appended
            -> RaftTime                 -- ^^ Last index up to which all entries are committed on leader
            -> [RaftLogEntry]    -- ^^ Entries to append
            -> IO (M.Map Name (Maybe MemberResult))
goAppendEntries cs cfg term prevTime commitTime entries = do
    let Just leader = clusterLeader cfg
        members = clusterMembers cfg
        timeout = (timeoutRpc $ configurationTimeouts cfg)
    results <- gcallWithTimeout cs members methodAppendEntries timeout
        $ encode $ AppendEntries leader term prevTime commitTime entries
    return $ mapResults results
    where
        mapResults results = M.map (\msg ->
            case msg of
                Just bytes -> let Right result = decode bytes in Just result
                _ -> Nothing) results

methodRequestVote :: String
methodRequestVote = "requestVote"

goRequestVote :: CallSite
                -> Configuration -- ^^ Cluster configuration
                -> Term     -- ^^ Candidate's term
                -> Name -- ^^ Candidate's id
                -> RaftTime -- ^^ `RaftTime` of candidate's last entry
                -> IO (M.Map Name (Maybe MemberResult))
goRequestVote cs cfg term candidate lastEntryTime = do
    let members = clusterMembers cfg
    timeout <- electionTimeout $ configurationTimeouts cfg
    results <- gcallWithTimeout cs members methodRequestVote timeout
        $ encode $ RequestVote candidate term lastEntryTime
    return $ mapResults results
    where
        mapResults results = M.map (\msg ->
            case msg of
                Just bytes -> let Right result = decode bytes in Just result
                _ -> Nothing) results

methodPerformAction :: String
methodPerformAction = "performAction"

goPerformAction :: CallSite
                    -> Configuration
                    -> Name
                    -> Action
                    -> IO (Maybe MemberResult)
goPerformAction cs cfg member action = do
    maybeMsg <- callWithTimeout cs member methodPerformAction (timeoutClientRpc $ configurationTimeouts cfg) $ encode action
    case maybeMsg of
        Just msg -> case decode msg of
                        Right result -> return $ Just result
                        Left _ -> return Nothing
        Nothing -> return Nothing

{-|
Wait for an 'AppendEntries' RPC to arrive, until 'rpcTimeout' expires. If one arrives,
process it, and return @True@.  If none arrives before the timeout, then return @False@.
-}
onAppendEntries :: Endpoint -> Configuration -> Name -> (AppendEntries -> IO MemberResult) -> IO (Maybe (Name,RaftTime))
onAppendEntries endpoint cfg server fn = do
    msg <- hearTimeout endpoint server methodAppendEntries (timeoutHeartbeat $ configurationTimeouts cfg)
    case msg of
        Just (bytes,reply) -> do
            let Right req = decode bytes
            result <- fn req
            reply $ encode result
            return $ Just (aeLeader req,aeCommittedTime req)
        Nothing -> return Nothing

{-|
Wait for an 'RequestVote' RPC to arrive, and process it when it arrives.
-}
onRequestVote :: Endpoint -> Name -> (RequestVote -> IO MemberResult) -> IO Bool
onRequestVote endpoint server fn = do
    (bytes,reply) <- hear endpoint server methodRequestVote
    let Right req = decode bytes
    result <- fn req
    reply $ encode result
    return $ memberActionSuccess result

{-|
Wait for a request from a client to perform an action, and process it when it arrives.
-}
onPerformAction :: Endpoint -> Name -> (Action -> Reply MemberResult -> IO ()) -> IO ()
onPerformAction endpoint member fn = do
    (bytes,reply) <- hear endpoint member methodPerformAction
    infoM _log $ "Heard performAction on " ++ member
    let Right action = decode bytes
    fn action (\response -> reply $ encode response)
    return ()
