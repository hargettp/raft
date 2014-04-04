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
-- (..... module description .....)
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft.Types (
    -- * General types
    ServerId,
    Term,
    Timeout,
    Timeouts(..),
    defaultTimeouts,
    timeouts,
    electionTimeout

) where

-- local imports

-- external imports

import Data.Serialize

import GHC.Generics

import Network.Endpoints

import qualified System.Random as R

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type Term = Int

type ServerId = Name

--------------------------------------------------------------------------------
-- Timeouts
--------------------------------------------------------------------------------

{-|
Type used for timeouts.  Mostly used for code clarity.
-}
type Timeout = Int

{-|
Defines the timeouts used for various aspects of the Raft protocol.
Different environments may have different performance characteristics,
and thus require different timeout values to operate correctly.
-}
data Timeouts = Timeouts {
    timeoutRpc :: Timeout,
    timeoutHeartbeat :: Timeout,
    timeoutPulse :: Timeout,
    timeoutElectionRange :: (Timeout,Timeout)
} deriving (Eq,Show,Generic)

instance Serialize Timeouts

{-|
Returns default timeouts generally expected to be useful
in real-world environments, largely based on original Raft paper.
-}
defaultTimeouts :: Timeouts 
defaultTimeouts = timeouts $ 150 * 1000

{-|
Returns timeouts scaled from the provided RPC timeout.

-}
timeouts :: Timeout -> Timeouts
timeouts rpc = 
    let heartbeat = 4 * rpc
        in Timeouts {
            timeoutRpc = rpc,
            timeoutHeartbeat = heartbeat,
            timeoutPulse = 3 * rpc,
            timeoutElectionRange = (5 * heartbeat,10 * heartbeat)
}

{-|
Return a new election timeout
-}
electionTimeout :: Timeouts -> IO Timeout
electionTimeout outs = R.randomRIO $ timeoutElectionRange outs
