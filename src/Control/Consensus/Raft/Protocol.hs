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

    MemberResult(..),
    createResult,

    -- * Client call
    goPerformAction,

    -- * Leader calls
    goAppendEntries,
    goRequestVote,
    onPerformAction,

    -- * Member handlers
    onAppendEntries,
    onRequestVote,

) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Log
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
    aeLeader :: ServerId,
    aeLeaderTerm :: Term,
    aePreviousTime :: RaftTime,
    aeCommittedTime :: RaftTime,
    aeEntries :: [RaftLogEntry]
} deriving (Eq,Show,Generic)

instance Serialize AppendEntries

data RequestVote = RequestVote {
        rvCandidate :: ServerId,
        rvCandidateTerm :: Term,
        rvCandidateLastEntryTime :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize RequestVote

data MemberResult = MemberResult {
    memberActionSuccess :: Bool,
    memberLeader :: Maybe ServerId,
    memberCurrentTerm :: Term,
    memberLastAppended :: RaftTime,
    memberLastCommitted :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize MemberResult

createResult :: (RaftLog l v) => Bool -> RaftState l v -> MemberResult
createResult success raft = MemberResult {
    memberActionSuccess = success,
    memberLeader = clusterLeader $ serverConfiguration $ serverState $ raftServer raft,
    memberCurrentTerm = raftCurrentTerm raft,
    memberLastAppended = lastAppended $ serverLog $ raftServer raft,
    memberLastCommitted = lastCommitted $ serverLog $ raftServer raft
}

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

goAppendEntries :: CallSite
            -> Configuration            -- ^^ Cluster configuration
            -> Name                     -- ^^ Member that is target of the call
            -> Term                     -- ^^ Leader's current term
            -> RaftTime                 -- ^^ `RaftTime` of entry just prior to the entries being appended
            -> RaftTime                    -- ^^ Last index up to which all entries are committed on leader
            -> [RaftLogEntry]    -- ^^ Entries to append
            -> IO (Maybe MemberResult)
goAppendEntries cs cfg member term prevTime commitTime entries = do
    let Just leader = clusterLeader cfg
    response <- callWithTimeout cs member methodAppendEntries (timeoutRpc $ configurationTimeouts cfg)
        $ encode $ AppendEntries leader term prevTime commitTime entries
    case response of
        Just bytes -> let Right results = decode bytes
                      in return $ Just results
        _ -> return Nothing

methodRequestVote :: String
methodRequestVote = "requestVote"

goRequestVote :: CallSite 
                -> Configuration -- ^^ Cluster configuration
                -> Term     -- ^^ Candidate's term
                -> ServerId -- ^^ Candidate's id
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
                    -> ServerId
                    -> Action
                    -> IO (Maybe MemberResult)
goPerformAction cs cfg member action = do
    maybeMsg <- callWithTimeout cs member methodPerformAction (timeoutRpc $ configurationTimeouts cfg) $ encode action
    case maybeMsg of
        Just msg -> case decode msg of
                        Right result -> return $ Just result
                        Left _ -> return Nothing
        Nothing -> return Nothing

{-|
Wait for an 'AppendEntries' RPC to arrive, until 'rpcTimeout' expires. If one arrives,
process it, and return @True@.  If none arrives before the timeout, then return @False@.
-}
onAppendEntries :: Endpoint -> Configuration -> ServerId -> (AppendEntries -> IO MemberResult) -> IO (Maybe RaftTime)
onAppendEntries endpoint cfg server fn = do
    msg <- hearTimeout endpoint server methodAppendEntries (timeoutHeartbeat $ configurationTimeouts cfg)
    case msg of
        Just (bytes,reply) -> do
            let Right req = decode bytes
            result <- fn req
            reply $ encode result
            return $ Just $ aeCommittedTime req
        Nothing -> return Nothing

{-|
Wait for an 'RequestVote' RPC to arrive, and process it when it arrives.
-}
onRequestVote :: Endpoint -> ServerId -> (RequestVote -> IO MemberResult) -> IO Bool
onRequestVote endpoint server fn = do
    (bytes,reply) <- hear endpoint server methodRequestVote
    let Right req = decode bytes
    result <- fn req
    reply $ encode result
    return $ memberActionSuccess result

{-|
Wait for a request from a client to perform an action, and process it when it arrives.
-}
onPerformAction :: Endpoint -> ServerId -> (Action -> IO MemberResult) -> IO ()
onPerformAction endpoint member fn = do
    (bytes,reply) <- hear endpoint member methodPerformAction
    infoM _log $ "Heard performAction on " ++ member
    let Right action = decode bytes
    response <- fn action
    reply $ encode response
    return ()
