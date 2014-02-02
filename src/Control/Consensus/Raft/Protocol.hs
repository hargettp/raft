{-# LANGUAGE DeriveGeneric #-}
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

    -- * Leader calls
    goAppendEntries,
    goRequestVote,

    -- * Member handlers
    onAppendEntries,
    onRequestVote,

    -- * Timeouts
    electionTimeout,
    heartbeatTimeout,
    pulseTimeout,
    rpcTimeout

) where

-- local imports

import Control.Consensus.Raft.Types
import Data.Log (Index)

-- external imports

import qualified Data.ByteString as B
import qualified Data.Map as M
import Data.Serialize

import GHC.Generics

import Network.Endpoints
import Network.RPC

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data AppendEntries =  AppendEntries {
    aeLeader :: ServerId,
    aeLeaderTerm :: Term,
    aePreviousIndex :: Index,
    aePreviousTerm :: Term,
    aeCommittedIndex :: Index,
    aeEntries :: [(Term,B.ByteString)]
} deriving (Eq,Show,Generic)

instance Serialize AppendEntries

data RequestVote = RequestVote {
        rvCandidate :: ServerId,
        rvCandidateTerm :: Term,
        rvCandidateLastEntryIndex :: Index,
        rvCandidateLastEntryTerm :: Term
} deriving (Eq,Show,Generic)

instance Serialize RequestVote

data RaftMessage = AppendEntriesRequest AppendEntries
    | AppendEntriesResponse Term Bool
    | RequestVoteRequest RequestVote
    | RequestVoteResponse Term Bool
    deriving (Eq,Show,Generic)

instance Serialize RaftMessage

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

goAppendEntries :: CallSite -> [Name]
            -> ServerId                 -- ^^ Leader
            -> Term                     -- ^^ Leader's current term
            -> Index                    -- ^^ Log index of entry just priot to the entries being appended
            -> Term                     -- ^^ Term of entry just priot to the entries being appended
            -> Index                    -- ^^ Last index up to which all entries are committed on leader
            -> [(Term,B.ByteString)]    -- ^^ Entries to append
            -> IO (M.Map Name (Maybe (Term,Bool)))
goAppendEntries cs members leader term prevLogIndex prevTerm commitIndex entries = do
    gcallWithTimeout cs members methodAppendEntries rpcTimeout
        $ AppendEntriesRequest $ AppendEntries leader term prevLogIndex prevTerm commitIndex entries

methodRequestVote :: String
methodRequestVote = "requestVote"

goRequestVote :: CallSite -> [Name]
                -> Term     -- ^^ Candidate's term
                -> ServerId -- ^^ Candidate's id
                -> Index    -- ^^ Index of candidate's last entry
                -> Term     -- ^^ Term of candidate's last entry
                -> IO (M.Map Name (Maybe (Term,Bool)))
goRequestVote cs members term candidate lastIndex lastTerm = do
    gcallWithTimeout cs members methodRequestVote rpcTimeout (term,candidate,lastIndex,lastTerm)

onAppendEntries :: Endpoint -> (AppendEntries -> IO (Term,Bool)) -> IO Bool
onAppendEntries endpoint fn = do
    msg <- selectMessageTimeout endpoint heartbeatTimeout isAppendEntries
    case msg of
        Just req -> do
            (term,success) <- fn req
            reply (aeLeader req) term success
            return success
        Nothing -> return False
    where
        isAppendEntries msg = case decode msg :: Either String RaftMessage of
                  Right (AppendEntriesRequest req)  ->
                      Just req
                  Right _ -> Nothing
                  Left _ -> Nothing
        reply leader term sucess = sendMessage_ endpoint leader $ encode $ AppendEntriesResponse term sucess

onRequestVote :: Endpoint -> (RequestVote -> IO (Term,Bool)) -> IO ()
onRequestVote endpoint fn = do
    msg <- selectMessageTimeout endpoint heartbeatTimeout isRequestVote
    case msg of
        Just req -> do
            (term,success) <- fn req
            reply (rvCandidate req) term success
            return ()
        Nothing -> return ()
    where
        isRequestVote msg = case decode msg :: Either String RaftMessage of
                  Right (RequestVoteRequest req)  ->
                      Just req
                  Right _ -> Nothing
                  Left _ -> Nothing
        reply leader term sucess = sendMessage_ endpoint leader $ encode $ RequestVoteResponse term sucess
--------------------------------------------------------------------------------
-- Timeouts
--------------------------------------------------------------------------------

{-|
Expected delay (in microseconds) for group rpc's to complete
-}
rpcTimeout :: Timeout
rpcTimeout = (50 * 1000)
-- rpcTimeout = (2000 * 1000)

{-|
Expected delay (in microseconds) between heartbeats
-}
heartbeatTimeout :: Timeout
heartbeatTimeout = (75 * 1000)

{-|
Maximum delay leader waits for a new message to process before
preparing heartbeat
-}
pulseTimeout :: Timeout
pulseTimeout = (50 * 1000)

{-|
Range for choosing an election timeout
-}
electionTimeoutRange :: (Timeout,Timeout)
electionTimeoutRange = (2 * heartbeatTimeout, 4 * heartbeatTimeout)

{-|
Return a new election timeout
-}
electionTimeout :: IO Timeout
electionTimeout = R.randomRIO electionTimeoutRange