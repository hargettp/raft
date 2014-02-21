-----------------------------------------------------------------------------
-- |
-- Module      :  TestRaft
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

module TestRaft (
    tests
) where

-- local imports

import Control.Consensus.Raft.Types

import IntServer

-- external imports

import Prelude hiding (log)

import Control.Applicative
import Control.Consensus.Raft
import Control.Concurrent
import Control.Concurrent.Async

import Network.Endpoints
import Network.Transport.Memory

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

tests :: [Test.Framework.Test]
tests = [
    testCase "cluster" testCluster
    ]

testCluster :: Assertion
testCluster = do
    let cfg = newConfiguration ["server1","server2","server3"]
    transport <- newMemoryTransport

    endpoint1 <- newEndpoint [transport]
    endpoint2 <- newEndpoint [transport]
    endpoint3 <- newEndpoint [transport]

    bindEndpoint_ endpoint1 "server1"
    bindEndpoint_ endpoint2 "server2"
    bindEndpoint_ endpoint3 "server3"

    server1 <- newIntServer cfg "server1" 0
    server2 <- newIntServer cfg "server2" 0
    server3 <- newIntServer cfg "server3" 0

    (result1,result2,result3) <- runConcurrently $ (,,)
        <$> Concurrently (runFor serverTimeout $ runConsensus endpoint1 server1)
        <*> Concurrently (runFor serverTimeout $ runConsensus endpoint2 server2)
        <*> Concurrently (runFor serverTimeout $ runConsensus endpoint3 server3)
    let servers = [result1,result2,result3]
        leaders = map (clusterLeader . serverConfiguration . serverState) servers
        results = map (serverData . serverState)  servers
    -- all results should be equal--and since we didn't perform any commands, should still be 0
    assertBool "All results should be equal" $ all (== 0) results
    assertBool ("All members should have same leader: " ++ (show leaders)) $ all (== (leaders !! 0)) leaders
    return ()

serverTimeout :: Timeout
serverTimeout = 2 * 1000000

{-|
Utility for running a server only for a defined period of time
-}
runFor :: Timeout -> IO a -> IO a
runFor timeout action = do
    actionAsync <- async action
    threadDelay timeout
    cancel actionAsync
    wait actionAsync



