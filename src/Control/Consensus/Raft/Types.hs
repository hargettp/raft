{-# LANGUAGE DeriveGeneric #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Types
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- Common types used in this implementation of the Raft algorithm.
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft.Types (
    -- * Configuration
    RaftConfiguration(..),
    mkRaftConfiguration,

    module Control.Consensus.Configuration,

    -- * General types
    Term,
    RaftTime(..),
    initialRaftTime,
    logIndex,
    logTerm,
    Timeout,
    Timeouts(..),
    defaultTimeouts,
    timeouts,
    electionTimeout

) where

-- local imports

import Data.Log

-- external imports

import Control.Consensus.Configuration

import Data.Serialize

import GHC.Generics

import Network.Endpoints

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{-|
A term is a phase in the execution of the Raft algorithm defined by a period
in which there is at most one leader. Members change terms when beginning a
new election, and after successfully winnning an election.
-}
type Term = Int

{-|
`RaftTime` captures a measure of how up to date a log is: it is
a combination of a 'Term' and 'Index'.
-}
data RaftTime = RaftTime Term Index deriving (Show,Eq,Ord,Generic)

{-|
Starting point for 'RaftTime': the time that is lowest than all other
valid 'RaftTime's.
-}
initialRaftTime :: RaftTime
initialRaftTime = RaftTime (-1) (-1)

{-|
Extracts the 'Index' from a 'RaftTime'.
-}
logIndex :: RaftTime -> Index
logIndex (RaftTime _ index) = index

{-|
Extracts the 'Term' from a 'RaftTime'.
-}
logTerm :: RaftTime -> Term
logTerm (RaftTime term _) = term

instance Serialize RaftTime

--------------------------------------------------------------------------------
-- Timeouts
--------------------------------------------------------------------------------

{-|
Type used for timeouts, measured in microseconds.  Mostly used for code clarity.
-}
type Timeout = Int

{-|
Defines the timeouts used for various aspects of the Raft protocol.
Different environments may have different performance characteristics,
and thus require different timeout values to operate correctly.
-}
data Timeouts = Timeouts {
    timeoutRpc :: Timeout, -- ^ maximum time to wait before deciding an RPC call has failed
    timeoutClientRpc :: Timeout, -- ^ maximum time clients waits before decinding an RPC call to a member has failed
    timeoutClientBackoff :: Timeout, -- ^ delay after trying to perform an action with all members before trying each member again
    timeoutHeartbeat :: Timeout, -- ^ expected time between heartbeats that prove the leader is still active
    timeoutPulse :: Timeout, -- ^ maximum length between pulses from the leader proving the leader is still active (must be less than heartbeat)
    timeoutElectionRange :: (Timeout,Timeout) -- ^ the range of times from which an election timeout will be selected
} deriving (Eq,Show,Generic)

instance Serialize Timeouts

{-|
Returns default timeouts generally expected to be useful
in real-world environments, largely based on original Raft paper.
-}
defaultTimeouts :: Timeouts
defaultTimeouts = timeouts $ 150 * 1000

{-|
Returns default timeouts scaled from the provided RPC timeout.

-}
timeouts :: Timeout -> Timeouts
timeouts rpc = 
    let heartbeat = 10 * rpc
        in Timeouts {
            timeoutRpc = rpc,
            timeoutClientRpc = 5 * rpc,
            timeoutClientBackoff = 4 * rpc,
            timeoutHeartbeat = heartbeat,
            timeoutPulse = 7 * rpc, -- must be less than the heartbeat
            timeoutElectionRange = (5 * heartbeat,10 * heartbeat)
}

{-|
Return a new election timeout
-}
electionTimeout :: Timeouts -> IO Timeout
electionTimeout outs = R.randomRIO $ timeoutElectionRange outs

--------------------------------------------------------------------------------
-- Cofiguration
--------------------------------------------------------------------------------

{- |
A 'RaftConfigurations' incorproates both an ordinary 'Configuration' but also a set
of 'Timeouts' for tuning the cluster's timing characteristics in the Raft algorithm.
-}
data RaftConfiguration = RaftConfiguration {
    clusterConfiguration :: Configuration,
    clusterTimeouts :: Timeouts
    } deriving (Generic,Show,Eq)

instance Serialize RaftConfiguration

{-|
Given a list of names, return a new 'RaftConfiguration' using 'defaultTimeouts' for
Raft algorithm execution.
-}
mkRaftConfiguration :: [Name] -> RaftConfiguration
mkRaftConfiguration participants = RaftConfiguration {
    clusterConfiguration = mkConfiguration participants,
    clusterTimeouts = defaultTimeouts
}
