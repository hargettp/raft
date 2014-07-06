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
-- Defines the low-level protocol describing the RPC messages that
-- pass members of a cluster governed by Raft.
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

{-|
A message type in the formal Raft algorithm, this message is used both
for sending a heartbeat from the leader to cluster members, but also
for replicating logged state to members as operations occur over time.
-}
data AppendEntries c = (Serialize c) => AppendEntries {
    -- | The leader who sent this message
    aeLeader :: Name,
    -- | The current term of the leader
    aeLeaderTerm :: Term,
    -- | The 'RaftTime' for the entry just prior to the entries sent in this message
    aePreviousTime :: RaftTime,
    -- | The latest 'RaftTime' for committed entries
    aeCommittedTime :: RaftTime,
    -- | The entries being appended in this request (if any)
    aeEntries :: [RaftLogEntry c]
}

instance (Serialize c) => Serialize (AppendEntries c) where
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

{-|
A message type in the formal Raft algorithm, this message is used to solicit
votes for a member wishing to take over the leadership role.
-}
data RequestVote = RequestVote {
        -- | The candidate's 'Name'
        rvCandidate :: Name,
        -- | The current term for the candidate
        rvCandidateTerm :: Term,
        -- | The latest 'RaftTime' for the last entry in the candidate's log
        rvCandidateLastEntryTime :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize RequestVote

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

{-|
Initiates an 'AppendEntries' RPC request to members of a cluster.
-}
goAppendEntries :: (Serialize e) => CallSite
            -> RaftConfiguration            -- ^^ Cluster configuration
            -> Term                     -- ^^ Leader's current term
            -> RaftTime                 -- ^^ `RaftTime` of entry just prior to the entries being appended
            -> RaftTime                 -- ^^ Last index up to which all entries are committed on leader
            -> [RaftLogEntry e]    -- ^^ Entries to append
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

{-|
Initiates a 'RequestVote' RPC to members of a cluster.
-}
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

{-|
Invoked by clients to request that a cluster perform an action. Not formally
part of the Raft algorithm, this extension enables client applications to
interact with a cluster managed via Raft.
-}
goPerformAction :: (Serialize e) => CallSite
                    -> RaftConfiguration
                    -> Name
                    -> RaftAction e
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
onAppendEntries :: (Serialize e) => Endpoint -> RaftConfiguration -> Name -> (AppendEntries e-> IO MemberResult) -> IO (Maybe Name)
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
onPerformAction :: (Serialize e) => Endpoint -> Name -> (RaftAction e -> Reply MemberResult -> IO ()) -> IO ()
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
