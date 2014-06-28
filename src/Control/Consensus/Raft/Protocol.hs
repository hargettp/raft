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
    onPassAction,
    onAppendEntries,
    onRequestVote

) where

-- local imports

import Control.Consensus.Raft.Actions
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Types

import Data.Log

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

data AppendEntries c = (Command c) => AppendEntries {
    aeLeader :: Name,
    aeLeaderTerm :: Term,
    aePreviousTime :: RaftTime,
    aeCommittedTime :: RaftTime,
    aeEntries :: [RaftLogEntry c]
}

deriving instance (Command c) => Eq (AppendEntries c)
deriving instance (Command c) => Show (AppendEntries c)

instance (Command c) => Serialize (AppendEntries c) where
    get = do
        leader <- get
        term <- get
        previous <- get
        committed <- get
        entries <- get
        return $ AppendEntries leader term previous committed entries
    put ae = do
        put $ aeLeader ae
        put $ aeLeaderTerm ae
        put $ aePreviousTime ae
        put $ aeCommittedTime ae
        put $ aeEntries ae

data RequestVote = RequestVote {
        rvCandidate :: Name,
        rvCandidateTerm :: Term,
        rvCandidateLastEntryTime :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize RequestVote

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

goAppendEntries :: (Command c) => CallSite
            -> RaftConfiguration            -- ^^ Cluster configuration
            -> Term                     -- ^^ Leader's current term
            -> RaftTime                 -- ^^ `RaftTime` of entry just prior to the entries being appended
            -> RaftTime                 -- ^^ Last index up to which all entries are committed on leader
            -> [RaftLogEntry c]    -- ^^ Entries to append
            -> IO (M.Map Name (Maybe MemberResult))
goAppendEntries cs cfg term prevTime commitTime entries = do
    let Just leader = clusterLeader $ clusterConfiguration cfg
        members = clusterMembers $ clusterConfiguration cfg
        timeout = (timeoutRpc $ clusterTimeouts cfg)
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
                -> RaftConfiguration -- ^^ Cluster configuration
                -> Term     -- ^^ Candidate's term
                -> Name -- ^^ Candidate's id
                -> RaftTime -- ^^ `RaftTime` of candidate's last entry
                -> IO (M.Map Name (Maybe MemberResult))
goRequestVote cs cfg term candidate lastEntryTime = do
    let members = clusterMembers $ clusterConfiguration cfg
    timeout <- electionTimeout $ clusterTimeouts cfg
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

goPerformAction :: (Command c) => CallSite
                    -> RaftConfiguration
                    -> Name
                    -> RaftAction c
                    -> IO (Maybe MemberResult)
goPerformAction cs cfg member action = do
    maybeMsg <- callWithTimeout cs member methodPerformAction (timeoutClientRpc $ clusterTimeouts cfg) $ encode action
    case maybeMsg of
        Just msg -> case decode msg of
                        Right result -> return $ Just result
                        Left _ -> return Nothing
        Nothing -> return Nothing

{-|
Wait for an 'AppendEntries' RPC to arrive, until 'rpcTimeout' expires. If one arrives,
process it, and return @True@.  If none arrives before the timeout, then return @False@.
-}
onAppendEntries :: (Command c) => Endpoint -> RaftConfiguration -> Name -> (AppendEntries c-> IO MemberResult) -> IO (Maybe Name)
onAppendEntries endpoint cfg server fn = do
    msg <- hearTimeout endpoint server methodAppendEntries (timeoutHeartbeat $ clusterTimeouts cfg)
    case msg of
        Just (bytes,reply) -> do
            let Right req = decode bytes
            result <- fn req
            reply $ encode result
            return $ Just $ aeLeader req
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
onPerformAction :: (Command c) => Endpoint -> Name -> (RaftAction c -> Reply MemberResult -> IO ()) -> IO ()
onPerformAction endpoint member fn = do
    (bytes,reply) <- hear endpoint member methodPerformAction
    infoM _log $ "Heard performAction on " ++ member
    let Right action = decode bytes
    fn action (\response -> reply $ encode response)
    return ()

{-|
Wait for a request from a client to perform an action, but pass on performing the action
-}
onPassAction :: Endpoint -> Name -> (Reply MemberResult -> IO ()) -> IO ()
onPassAction endpoint member fn = do
    (_,reply) <- hear endpoint member methodPerformAction
    infoM _log $ "Heard performAction on " ++ member
    fn $ \response -> reply $ encode response
    return ()
