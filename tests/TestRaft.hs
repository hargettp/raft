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
    tests,
    troubleshoot
) where

-- local imports

import Control.Consensus.Raft
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Members
import Control.Consensus.Raft.Protocol

import IntServer

-- external imports

import Prelude hiding (log)

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
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
    testCase "3cluster" test3Cluster,
    testCase "3cluster-stability" test3ClusterStability,
    testCase "5cluster" test5Cluster,
    testCase "5cluster-stability" test5ClusterStability,
    testCase "client" testClient,
    testCase "performAction" testPerformAction,
    testCase "goPerformAction" testGoPerformAction,
    testCase "clientPerformAction" testClientPerformAction,
    testCase "withClientPerformAction" testWithClientPerformAction
    ]

troubleshoot :: IO () -> IO ()
troubleshoot fn = do
    finally (do
        updateGlobalLogger rootLoggerName (setLevel INFO)
        fn) (updateGlobalLogger rootLoggerName (setLevel WARNING))

test3Cluster :: Assertion
test3Cluster = do
    transport <- newMemoryTransport
    let cfg = newTestConfiguration ["server1","server2","server3"]
    with3Servers  transport cfg $ \vRafts -> do
        leader <- waitForLeader 5 (1 :: Integer) vRafts
        leaders <- allLeaders vRafts
        infoM _log $ "Leaders are " ++ (show leaders)
        _ <- checkForConsistency (1 :: Integer) leader vRafts
        return ()

test3ClusterStability :: Assertion
test3ClusterStability = do
    transport <- newMemoryTransport
    let cfg = newTestConfiguration ["server1","server2","server3"]
    with3Servers  transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        pause >> pause
        _ <- checkForConsistency (2 :: Integer) firstLeader vRafts
        return ()

test5Cluster :: Assertion
test5Cluster = do
    transport <- newMemoryTransport
    let cfg = newTestConfiguration ["server1","server2","server3","server4","server5"]
    with5Servers  transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        return ()

test5ClusterStability :: Assertion
test5ClusterStability = do
    transport <- newMemoryTransport
    let cfg = newTestConfiguration ["server1","server2","server3","server4","server5"]
    with5Servers  transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        pause >> pause >> pause
        _ <- checkForConsistency (2 :: Integer) firstLeader vRafts
        return ()

testClient :: Assertion
testClient = do
    transport <- newMemoryTransport
    let cfg = newTestConfiguration ["server1","server2","server3"]
    with3Servers  transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                performAction client $ Cmd $ encode $ Add 1)
        case errOrResult of
            Right clientResult -> do
                let RaftTime _ clientIndex = clientResult
                assertBool "Client index should be 0" $ clientIndex == 0
            Left _ -> do
                assertBool "Performing action failed" False

testPerformAction :: Assertion
testPerformAction = do
    let client = "client1"
        server = "server1"
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ reply -> do
            reply MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 0,
                memberLastAppended = RaftTime 0 0,
                memberLastCommitted = RaftTime 0 0
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ encode $ Add 1
        outs = defaultTimeouts
    Just msg <- callWithTimeout cs server "performAction" (timeoutRpc outs) $ encode action
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
        onPerformAction endpoint server $ \_ reply -> do
            infoM _log $ "Returning result"
            reply MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 0,
                memberLastAppended = RaftTime 0 0,
                memberLastCommitted = RaftTime 0 0
                }
    -- we have to wait a bit for server to start
    pause
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ encode $ Add 1
        cfg = (newTestConfiguration [server]) {configurationLeader = Just server}
    response <- goPerformAction cs cfg server action
    case response of
        Just result -> assertBool "Result should be true" $ memberActionSuccess result
        Nothing -> assertBool "No response" False

testClientPerformAction :: Assertion
testClientPerformAction = do
    let client = "client1"
        server = "server1"
        cfg = newTestConfiguration [server]
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ reply -> do
            reply MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 1,
                memberLastAppended = RaftTime 1 1,
                memberLastCommitted = RaftTime 1 1
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let raftClient = newClient endpoint client cfg
    let action = Cmd $ encode $ Add 1
    result <- performAction raftClient action
    assertBool "Result should be true" $ result == RaftTime 1 1

testWithClientPerformAction :: Assertion
testWithClientPerformAction = do
    let client = "client1"
        server = "server1"
        cfg = newTestConfiguration [server]
    transport <- newMemoryTransport
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        onPerformAction endpoint server $ \_ reply -> do
            reply MemberResult {
                memberActionSuccess = True,
                memberLeader = Nothing,
                memberCurrentTerm = 1,
                memberLastAppended = RaftTime 1 1,
                memberLastCommitted = RaftTime 1 1
                }
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let action = Cmd $ encode $ Add 1
    result <- withClient transport client cfg $ \raftClient -> do
        pause
        performAction raftClient action
    assertBool "Result should be true" $ result == RaftTime 1 1

--------------------------------------------------------------------------------
-- helpers
--------------------------------------------------------------------------------

checkForConsistency :: Integer -> Maybe Name -> [TVar (RaftContext IntLog IntState)] -> IO (Maybe Name)
checkForConsistency run possibleLeader vRafts = do
    servers <- mapM (\vRaft -> do
        raft <- atomically $ readTVar vRaft
        return $ raftServer raft) vRafts
    let leaders = map (clusterLeader . serverConfiguration . serverState) servers
        results = map (serverData . serverState)  servers
    -- all results should be equal--and since we didn't perform any commands, should still be 0
    assertBool ((show run) ++ ": All results should be equal") $ all (== (IntState 0)) results
    assertBool ((show run) ++ ": All members should have same leader: " ++ (show leaders)) $ all (== (leaders !! 0)) leaders
    assertBool ((show run) ++ ": There must be a leader " ++ (show leaders)) $ all (/= Nothing) leaders
    let leader = (leaders !! 0)
    case possibleLeader of
        Just _ -> if all (== possibleLeader) leaders
                            then return leader
                            else return Nothing
        Nothing -> if all (== leader) leaders
                        then return leader
                        else return Nothing

waitForLeader :: Integer -> Integer -> [TVar (RaftContext IntLog IntState)] -> IO (Maybe Name)
waitForLeader maxCount attempt vRafts = do
    servers <- mapM (\vRaft -> do
        raft <- atomically $ readTVar vRaft
        return $ raftServer raft) vRafts
    let leaders = map (clusterLeader . serverConfiguration . serverState) servers
        leader = leaders !! 0
    if maxCount <= 0
        then do
            assertBool ("No leader found after " ++ (show (attempt - 1)) ++ " rounds: " ++ (show leaders)) False
            return Nothing
        else do
            pause
            if (leader /= Nothing) && (all (== leader) leaders)
                then do
                    let msg = "After " ++ (show attempt) ++ " rounds, the leader is " ++ (show leader)
                    if attempt > 2
                        then warningM _log msg
                        else infoM _log msg
                    return leader
                else
                    waitForLeader (maxCount - 1) (attempt + 1) vRafts

allLeaders :: [TVar (RaftContext IntLog IntState)] -> IO [Maybe Name]
allLeaders vRafts = do
    servers <- mapM (\vRaft -> do
        raft <- atomically $ readTVar vRaft
        return $ raftServer raft) vRafts
    let leaders = map (clusterLeader . serverConfiguration . serverState) servers
    return leaders

withClient :: Transport -> Name -> Configuration -> (Client -> IO a) -> IO a
withClient transport name cfg fn = do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    let client = newClient endpoint name cfg
    fn client

newTestConfiguration :: [Name] -> Configuration
newTestConfiguration members = (newConfiguration members) {configurationTimeouts = testTimeouts}

pause :: IO ()
pause = threadDelay serverTimeout

testTimeouts :: Timeouts
testTimeouts = timeouts (25 * 1000)

-- We need this to be high enough that members
-- can complete their election processing (that's the longest timeout we have)
-- before the test itself completes--otherwise we occasionally get spurious
-- failures, as a member is still electing when the test ends, resulting
-- in test failures, because not all members have agreed on the same leader
serverTimeout :: Timeout
serverTimeout = 2 * (snd $ timeoutElectionRange testTimeouts)

{-|
Utility for running a server only for a defined period of time
-}
withServer :: Transport -> Configuration -> Name -> (IntRaft -> IO ()) -> IO ()
withServer transport cfg name fn = do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    server <- mkIntServer cfg name 0
    finally (withConsensus endpoint server fn)
        (unbindEndpoint_ endpoint name)

with3Servers :: Transport -> Configuration -> ([IntRaft] -> IO ()) -> IO ()
with3Servers transport cfg fn = 
    let names = clusterMembers cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 -> fn $ [vRaft1] ++ [vRaft2] ++ [vRaft3]

with5Servers :: Transport -> Configuration -> ([IntRaft] -> IO ()) -> IO ()
with5Servers transport cfg fn = 
    let names = clusterMembers cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 ->
        withServer transport cfg (names !! 3) $ \vRaft4 ->
        withServer transport cfg (names !! 4) $ \vRaft5 -> fn $ [vRaft1] ++ [vRaft2] ++ [vRaft3] ++ [vRaft4] ++ [vRaft5]
