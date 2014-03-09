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

import Control.Consensus.Raft
import Control.Consensus.Raft.Client
import Control.Consensus.Raft.Protocol
import Control.Consensus.Raft.Types

import IntServer

-- external imports

import Prelude hiding (log)

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception

import Data.Serialize

import Network.Endpoints
import Network.RPC
import Network.Transport.Memory

import System.Log.Logger

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "test.raft"

tests :: [Test.Framework.Test]
tests = [
    testCase "cluster" testCluster,
    testCase "client" testClient,
    testCase "performAction" testPerformAction,
    testCase "goPerformAction" testGoPerformAction,
    testCase "clientPerformAction" testClientPerformAction,
    testCase "runClientPerformAction" testRunClientPerformAction
    ]

testCluster :: Assertion
testCluster = do
    transport <- newMemoryTransport
    let cfg = newConfiguration ["server1","server2","server3"]
    (result1,result2,result3) <- run3NodeCluster transport cfg
    let servers = [result1,result2,result3]
        leaders = map (clusterLeader . serverConfiguration . serverState) servers
        results = map (serverData . serverState)  servers
    -- all results should be equal--and since we didn't perform any commands, should still be 0
    assertBool "All results should be equal" $ all (== 0) results
    assertBool ("All members should have same leader: " ++ (show leaders)) $ all (== (leaders !! 0)) leaders
    assertBool ("There must be a leader " ++ (show leaders)) $ all (/= Nothing) leaders
    return ()

testClient :: Assertion
testClient = do
    transport <- newMemoryTransport
    let cfg = newConfiguration ["server1","server2","server3"]
    Right clientResult <- race (run3NodeCluster transport cfg)
                            (runClient transport "client1" cfg $ \client -> do
                                threadDelay (500 * 1000)
                                performAction client $ Cmd $ encode $ Add 1)
    assertBool "Client index should be -1" $ clientResult == -1

testPerformAction :: Assertion
testPerformAction = do
    let client = "client1"
        server = "server1"
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ -> do
            return MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 0,
                memberLastAppended = 0,
                memberLastCommitted = 0
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ encode $ Add 1
    Just msg <- callWithTimeout cs server "performAction" rpcTimeout $ encode action
    let Right result = decode msg
    assertBool "Result should be true" $ memberActionSuccess result

testGoPerformAction :: Assertion
testGoPerformAction = do
    let client = "client1"
        server = "server1"
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ -> do
            infoM _log $ "Returning result"
            return MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 0,
                memberLastAppended = 0,
                memberLastCommitted = 0
                }
    -- we have to wait a bit for server to start
    threadDelay $ 1000 * 1000
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ encode $ Add 1
    response <- goPerformAction cs server action
    case response of
        Just result -> assertBool "Result should be true" $ memberActionSuccess result
        Nothing -> assertBool "No response" False

testClientPerformAction :: Assertion
testClientPerformAction = do
    let client = "client1"
        server = "server1"
        cfg = newConfiguration [server]
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ -> do
            return MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 1,
                memberLastAppended = 1,
                memberLastCommitted = 1
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let raftClient = newClient endpoint client cfg
    let action = Cmd $ encode $ Add 1
    result <- performAction raftClient action
    assertBool "Result should be true" $ result == 1

testRunClientPerformAction :: Assertion
testRunClientPerformAction = do
    let client = "client1"
        server = "server1"
        cfg = newConfiguration [server]
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ -> do
            return MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 1,
                memberLastAppended = 1,
                memberLastCommitted = 1
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let action = Cmd $ encode $ Add 1
    result <- runClient transport client cfg $ \raftClient -> do
        performAction raftClient action
    assertBool "Result should be true" $ result == 1

--------------------------------------------------------------------------------
-- helpers
--------------------------------------------------------------------------------

runClient :: Transport -> Name -> Configuration -> (Client -> IO a) -> IO a
runClient transport name cfg fn = catch (do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    let client = newClient endpoint name cfg
    fn client) (\e -> do 
        errorM _log $ "Client error: " ++ (show (e :: SomeException))
        return $ throw e)

serverTimeout :: Timeout
serverTimeout = 5 * 1000000

{-|
Utility for running a server only for a defined period of time
-}
runFor :: Timeout -> Transport -> Configuration -> ServerId -> IO (RaftServer IntLog Int)
runFor timeout transport cfg name  = do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    server <- newIntServer cfg name 0
    actionAsync <- async $ runConsensus endpoint server
    threadDelay timeout
    cancel actionAsync
    result <- wait actionAsync
    unbindEndpoint_ endpoint name
    return result

run3NodeCluster :: Transport -> Configuration -> IO (RaftServer IntLog Int,RaftServer IntLog Int,RaftServer IntLog Int)
run3NodeCluster transport cfg = do
    let servers = clusterMembers cfg
    runConcurrently $ (,,)
        <$> Concurrently (runFor serverTimeout transport cfg $ servers !! 0)
        <*> Concurrently (runFor serverTimeout transport cfg $ servers !! 1)
        <*> Concurrently (runFor serverTimeout transport cfg $ servers !! 2)
