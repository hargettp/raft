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

    -- * Leader calls
    appendEntries,
    requestVote,

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

-- external imports

import qualified Data.ByteString as B
import qualified Data.Map as M

import Network.Endpoints
import Network.RPC

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

methodAppendEntries :: String
methodAppendEntries = "appendEntries"

appendEntries :: CallSite -> [Name]
            -> ServerId                 -- ^^ Leader
            -> Term                     -- ^^ Leader's current term
            -> Index                    -- ^^ Log index of entry just priot to the entries being appended
            -> Term                     -- ^^ Term of entry just priot to the entries being appended
            -> Index                    -- ^^ Last index up to which all entries are committed on leader
            -> [(Term,B.ByteString)]    -- ^^ Entries to append
            -> IO (M.Map Name (Maybe (Term,Bool)))
appendEntries cs members leader term prevLogIndex prevTerm commitIndex entries = do
    gcallWithTimeout cs members methodAppendEntries rpcTimeout (leader,term,prevLogIndex,prevTerm,commitIndex,entries)

methodRequestVote :: String
methodRequestVote = "requestVote"

requestVote :: CallSite -> [Name]
                -> Term     -- ^^ Candidate's term
                -> ServerId -- ^^ Candidate's id
                -> Index    -- ^^ Index of candidate's last entry
                -> Term     -- ^^ Term of candidate's last entry
                -> IO (M.Map Name (Maybe (Term,Bool)))
requestVote cs members term candidate lastIndex lastTerm = do
    gcallWithTimeout cs members methodRequestVote rpcTimeout (term,candidate,lastIndex,lastTerm)

onAppendEntries :: Endpoint -> Name -> ((ServerId, Term, Index, Term, Index, [(Term,B.ByteString)])-> IO (Term,Bool)) -> IO HandleSite
onAppendEntries endpoint member fn = do
    handle endpoint member methodAppendEntries fn

onRequestVote :: Endpoint -> Name -> ((Term, ServerId, Index, Term) -> IO (Term,Bool))-> IO HandleSite
onRequestVote endpoint member fn = do
    handle endpoint member methodRequestVote fn

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