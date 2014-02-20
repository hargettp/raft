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

    async1 <- async $ runConsensus endpoint1 server1
    async2 <- async $ runConsensus endpoint2 server2
    async3 <- async $ runConsensus endpoint3 server3
    sleep <- async $ do
        threadDelay 2000000
        return server1
    _ <- waitAnyCancel [sleep,async1,async2,async3]

    return ()


