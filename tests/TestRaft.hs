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

import qualified Data.Map as M
import Data.Serialize

import Network.Endpoints
import Network.RPC
import Network.Transport.Memory
import Network.Transport.TCP
import Network.Transport.UDP

import System.Environment
import System.Log.Logger

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

import Text.Printf

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "test.raft"

tests :: IO [Test.Framework.Test]
tests = let servers7 = ["server1","server2","server3","server4","server5","server6","server7"]
            servers5 = take 5 servers7
            servers3 = take 3 servers7
            servers1 = take 1 servers7
            mem1Cfg = newTestConfiguration servers1
            mem3Cfg = newTestConfiguration servers3
            socket1Cfg = newSocketTestConfiguration servers1
            socket3Cfg = newSocketTestConfiguration servers3
            mem5Cfg = newTestConfiguration servers5
            socket5Cfg = newSocketTestConfiguration servers5
            mem7Cfg = newTestConfiguration servers7
            socket7Cfg = newSocketTestConfiguration servers7
            resolver = resolverFromList[
                    ("server1","localhost:20001"),
                    ("server2","localhost:20002"),
                    ("server3","localhost:20003"),
                    ("server4","localhost:20004"),
                    ("server5","localhost:20005"),
                    ("server6","localhost:20006"),
                    ("server7","localhost:20007"),
                    ("client1","localhost:30000")]
            memTests = (testsFor1 "mem" newMemoryTransport mem1Cfg)
                ++ (testsFor3 "mem" newMemoryTransport mem3Cfg)
                ++ (testsFor5 "mem" (newTCPTransport resolver) mem5Cfg)
                ++ (testsFor7 "mem" (newTCPTransport resolver) mem7Cfg)
            udpTests = (testsFor1 "udp" (newUDPTransport resolver) socket1Cfg)
               ++ (testsFor3 "udp" (newUDPTransport resolver) socket3Cfg)
               ++ (testsFor5 "udp" (newUDPTransport resolver) socket5Cfg)
               ++ (testsFor7 "udp" (newUDPTransport resolver) socket7Cfg)
            limitedUdpTests = [testCase (nameTest "udp" "3cluster") $ test3Cluster (newUDPTransport resolver) socket3Cfg]
            tcpTests = (testsFor1 "tcp" (newTCPTransport resolver) socket1Cfg) ++
               (testsFor3 "tcp" (newTCPTransport resolver) socket3Cfg) ++
               (testsFor5 "tcp" (newTCPTransport resolver) socket5Cfg) ++
               (testsFor7 "tcp" (newTCPTransport resolver) socket7Cfg)
            limitedTcpTests = [testCase (nameTest "tcp" "3cluster") $ test3Cluster (newTCPTransport resolver) socket3Cfg]
            in do
                raftTransport <- lookupEnv "RAFT_TRANSPORT"
                case raftTransport of
                    Just "mem" -> return memTests
                    Just "udp" -> return udpTests
                    Just "tcp" -> return tcpTests
                    _ -> return $ memTests ++ limitedUdpTests ++ limitedTcpTests

testsFor1 :: String -> (IO Transport) -> RaftConfiguration -> [Test.Framework.Test]
testsFor1 base transportF cfg =  [
        testCase (nameTest base "performAction") $ testPerformAction transportF,
        testCase (nameTest base "goPerformAction") $ testGoPerformAction transportF,
        testCase (nameTest base "clientPerformAction") $ testClientPerformAction transportF cfg,
        testCase (nameTest base "withClientPerformAction") $ testWithClientPerformAction transportF cfg
        ]

testsFor3 :: String -> (IO Transport) -> RaftConfiguration -> [Test.Framework.Test]
testsFor3 base transportF cfg =  [
        testCase (nameTest base "3cluster") $ test3Cluster transportF cfg,
        testCase (nameTest base "3cluster-1nonparticipant") $ test3Cluster1NonParticipant transportF cfg,
        testCase (nameTest base "3cluster-add1") $ test3ClusterAdd1 transportF cfg,
        testCase (nameTest base "3cluster-stability") $ test3ClusterStability transportF cfg,
        testCase (nameTest base "3cluster-1nonparticipant-stability") $ test3Cluster1NonParticipantStability transportF cfg,
        testCase (nameTest base "client") $ testClient transportF cfg,
        testCase (nameTest base "consistency") $ testConsistency transportF cfg,
        testCase (nameTest base "2consistency") $ test2Consistency transportF cfg,
        testCase (nameTest base "3consistency") $ test3Consistency transportF cfg,
        testCase (nameTest base "10consistency") $ test10Consistency transportF cfg
        ]

testsFor5 :: String -> (IO Transport) -> RaftConfiguration -> [Test.Framework.Test]
testsFor5 base transportF cfg = [
        testCase (nameTest base "5cluster") $ test5Cluster transportF cfg,
        testCase (nameTest base "5cluster-stability") $ test5ClusterStability transportF cfg,
        testCase (nameTest base "5cluster-10consistency") $ test5Cluster10Consistency transportF cfg
        ]

testsFor7 :: String -> (IO Transport) -> RaftConfiguration -> [Test.Framework.Test]
testsFor7 base transportF cfg = [
        testCase (nameTest base "7cluster-10consistency") $ test7Cluster10Consistency transportF cfg
        ]

nameTest :: String -> String -> String
nameTest base text = base ++ "-" ++ text

troubleshoot :: IO () -> IO ()
troubleshoot fn = do
    finally (do
        updateGlobalLogger rootLoggerName (setLevel INFO)
        fn) (updateGlobalLogger rootLoggerName (setLevel WARNING))

test3Cluster :: (IO Transport) -> RaftConfiguration -> Assertion
test3Cluster transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        leader <- waitForLeader 5 (1 :: Integer) vRafts
        leaders <- allLeaders vRafts
        infoM _log $ "Leaders are " ++ (show leaders)
        _ <- checkForConsistency (1 :: Integer) leader vRafts
        return ()

test3Cluster1NonParticipant :: (IO Transport) -> RaftConfiguration -> Assertion
test3Cluster1NonParticipant transportF cfg = do
    transport <- transportF
    with3Servers1NonParticipant transport cfg "server4" $ \vRafts -> do
        let mRafts = take 3 vRafts
        leader <- waitForLeader 5 (1 :: Integer) mRafts
        leaders <- allLeaders mRafts
        infoM _log $ "Leaders are " ++ (show leaders)
        _ <- checkForConsistency (1 :: Integer) leader mRafts
        return ()

test3ClusterAdd1 :: (IO Transport) -> RaftConfiguration -> Assertion
test3ClusterAdd1 transportF cfg = do
    transport <- transportF
    with3Servers1NonParticipant transport cfg "server4" $ \vRafts -> do
        let mRafts = take 3 vRafts
        leader <- waitForLeader 5 (1 :: Integer) mRafts
        leaders <- allLeaders mRafts
        infoM _log $ "Leaders are " ++ (show leaders)
        _ <- checkForConsistency (1 :: Integer) leader mRafts
        withClient transport "client1" cfg $ \client -> do
            RaftTime _ clientIndex <- performAction client $ ((Cfg $ AddParticipants ["server4"]) :: RaftAction IntCommand)
            assertBool (printf "Client index should be 0: %v" (show clientIndex)) $ clientIndex == 0
            _ <- waitForLeader 5 (2 :: Integer) vRafts
            newLeaders <- allLeaders vRafts
            infoM _log $ printf "New leaders are %v" (show newLeaders)
            newLastCommitted <- allLastCommitted vRafts
            infoM _log $ printf "Last committed indexes are %v" (show newLastCommitted)
            _ <- checkForConsistency (1 :: Integer) leader vRafts
            return ()

test3ClusterStability :: (IO Transport) -> RaftConfiguration -> Assertion
test3ClusterStability transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        pause >> pause
        _ <- checkForConsistency (2 :: Integer) firstLeader vRafts
        return ()

test3Cluster1NonParticipantStability :: (IO Transport) -> RaftConfiguration -> Assertion
test3Cluster1NonParticipantStability transportF cfg = do
    transport <- transportF
    with3Servers1NonParticipant  transport cfg "server4" $ \vRafts -> do
        let mRafts = take 3 vRafts
        firstLeader <- waitForLeader 5 (1 :: Integer) mRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader mRafts
        pause >> pause
        _ <- checkForConsistency (2 :: Integer) firstLeader mRafts
        return ()

test5Cluster :: (IO Transport) -> RaftConfiguration -> Assertion
test5Cluster transportF cfg = do
    transport <- transportF
    with5Servers transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        return ()

test5ClusterStability :: (IO Transport) -> RaftConfiguration -> Assertion
test5ClusterStability transportF cfg = do
    transport <- transportF
    with5Servers transport cfg $ \vRafts -> do
        firstLeader <- waitForLeader 5 (1 :: Integer) vRafts
        _ <- checkForConsistency (1 :: Integer) firstLeader vRafts
        pause >> pause >> pause
        _ <- checkForConsistency (2 :: Integer) firstLeader vRafts
        return ()

testClient :: (IO Transport) -> RaftConfiguration -> Assertion
testClient transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                performAction client $ Cmd $ Add 1)
        case errOrResult of
            Right clientResult -> do
                let RaftTime _ clientIndex = clientResult
                assertBool "Client index should be 0" $ clientIndex == 0
            Left _ -> do
                assertBool "Performing action failed" False

testConsistency :: (IO Transport) -> RaftConfiguration -> Assertion
testConsistency transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Add 1
                                assertBool (printf "Client index should be 0: %v" (show clientIndex)) $ clientIndex == 0
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 1)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

test2Consistency :: (IO Transport) -> RaftConfiguration -> Assertion
test2Consistency transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                _ <- performAction client $ Cmd $ Add 3
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Multiply 5
                                assertBool  (printf "Client index should be 1: %v" (show clientIndex)) $ clientIndex == 1
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 15)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

test3Consistency :: (IO Transport) -> RaftConfiguration -> Assertion
test3Consistency transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Subtract 2
                                assertBool  (printf "Client index should be 2: %v" (show clientIndex)) $ clientIndex == 2
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 13)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

test10Consistency :: (IO Transport) -> RaftConfiguration -> Assertion
test10Consistency transportF cfg = do
    transport <- transportF
    with3Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Add 3
                                assertBool  (printf "Client index should be 9: %v" (show clientIndex)) $ clientIndex == 9
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 406)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

test5Cluster10Consistency :: (IO Transport) -> RaftConfiguration -> Assertion
test5Cluster10Consistency transportF cfg = do
    transport <- transportF
    with5Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 5 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 5 (1 :: Integer) vRafts
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Add 3
                                assertBool  (printf "Client index should be 9: %v" (show clientIndex)) $ clientIndex == 9
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 406)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

test7Cluster10Consistency :: (IO Transport) -> RaftConfiguration -> Assertion
test7Cluster10Consistency transportF cfg = do
    transport <- transportF
    with7Servers transport cfg $ \vRafts -> do
        pause
        errOrResult <- race (do 
                _ <- waitForLeader 10 (1 :: Integer) vRafts
                threadDelay $ 30 * 1000 * 1000
                return ())
            (withClient transport "client1" cfg $ \client -> do
                                _ <- waitForLeader 10 (1 :: Integer) vRafts
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                _ <- performAction client $ Cmd $ Add 3
                                _ <- performAction client $ Cmd $ Multiply 5
                                _ <- performAction client $ Cmd $ Subtract 2
                                RaftTime _ clientIndex <- performAction client $ Cmd $ Add 3
                                assertBool  (printf "Client index should be 9: %v" (show clientIndex)) $ clientIndex == 9
                                -- pause -- have to wait for synchronization to occur
                                threadDelay $ 2 * 1000 * 1000
                                states <- allStates vRafts
                                assertBool "" $ all (== (IntState 406)) states
                                checkForConsistency_ vRafts)
        case errOrResult of
            Right _ -> assertBool "" True
            Left _ -> assertBool "Performing action failed" False

testPerformAction :: (IO Transport) -> Assertion
testPerformAction transportF = do
    transport <- transportF
    let client = "client1"
        server = "server1"
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        finally (do
            onPassAction endpoint server $ \reply -> do
                reply MemberResult {
                    memberActionSuccess = True,
                    memberLeader = Nothing,
                    memberCurrentTerm = 0,
                    memberLastAppended = RaftTime 0 0,
                    memberLastCommitted = RaftTime 0 0
                    })
            (unbindEndpoint_ endpoint server)
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ Add 1
        outs = defaultTimeouts
    finally (do 
        Just msg <- callWithTimeout cs server "performAction" (timeoutRpc outs) $ encode action
        let Right result = decode msg
        assertBool "Result should be true" $ memberActionSuccess result)
        (unbindEndpoint_ endpoint client)

testGoPerformAction :: (IO Transport) -> Assertion
testGoPerformAction transportF = do
    transport <- transportF
    let client = "client1"
        server = "server1"
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        finally (onPassAction endpoint server $ \reply -> do
                infoM _log $ "Returning result"
                reply MemberResult {
                    memberActionSuccess = True,
                    memberLeader = Nothing,
                    memberCurrentTerm = 0,
                    memberLastAppended = RaftTime 0 0,
                    memberLastCommitted = RaftTime 0 0
                    })
                (unbindEndpoint_ endpoint server)
    -- we have to wait a bit for server to start
    pause
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let cs = newCallSite endpoint client
        action = Cmd $ Add 1
        raftcfg = newTestConfiguration [server]
        cfg = raftcfg {
            clusterConfiguration = (clusterConfiguration raftcfg) {configurationLeader = Just server
            }}
    finally (do
            response <- goPerformAction cs cfg server action
            case response of
                Just result -> assertBool "Result should be true" $ memberActionSuccess result
                Nothing -> assertBool "No response" False)
        (unbindEndpoint_ endpoint client)

testClientPerformAction :: (IO Transport) -> RaftConfiguration -> Assertion
testClientPerformAction transportF cfg = do
    transport <- transportF
    let client = "client1"
        server = "server1"
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        finally (onPassAction endpoint server $ \reply -> do
                reply MemberResult {
                    memberActionSuccess = True,
                    memberLeader = Nothing,
                    memberCurrentTerm = 1,
                    memberLastAppended = RaftTime 1 1,
                    memberLastCommitted = RaftTime 1 1
                    })
            (unbindEndpoint_ endpoint server)
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint client
    let raftClient = newClient endpoint client cfg
    let action = Cmd $ Add 1
    finally (do
                result <- performAction raftClient action
                assertBool "Result should be true" $ result == RaftTime 1 1)
                (unbindEndpoint_ endpoint client)

testWithClientPerformAction :: (IO Transport) -> RaftConfiguration -> Assertion
testWithClientPerformAction transportF cfg = do
    transport <- transportF
    let client = "client1"
        server = "server1"
    _ <- async $ do
        endpoint <- newEndpoint [transport]
        bindEndpoint_ endpoint server
        finally (onPassAction endpoint server $ \reply -> do
                reply MemberResult {
                    memberActionSuccess = True,
                    memberLeader = Nothing,
                    memberCurrentTerm = 1,
                    memberLastAppended = RaftTime 1 1,
                    memberLastCommitted = RaftTime 1 1
                    })
                (unbindEndpoint_ endpoint server)
    let action = Cmd $ Add 1
    result <- withClient transport client cfg $ \raftClient -> do
        pause
        performAction raftClient action
    assertBool "Result should be true" $ result == RaftTime 1 1

--------------------------------------------------------------------------------
-- helpers
--------------------------------------------------------------------------------

checkForConsistency_ :: [Raft IntLog IntCommand IntState] -> IO (Maybe Name)
checkForConsistency_ rafts = checkForConsistency 1 Nothing rafts

checkForConsistency :: Integer -> Maybe Name -> [Raft IntLog IntCommand IntState] -> IO (Maybe Name)
checkForConsistency run possibleLeader vRafts = do
    rafts <- mapM (\vRaft -> atomically $ readTVar $ raftContext vRaft) vRafts
    let leaders = map (clusterLeader . clusterConfiguration . raftStateConfiguration . raftState) rafts
        results = map (raftStateData . raftState) rafts
        indexes = map (lastCommitted . raftLog) rafts
    assertBool (printf "%d: Most results should be equal" run) $ hasCommon results
    assertBool (printf "%d: Most log indexes should be equal" run) $ hasCommon indexes
    assertBool (printf "%d: Most members should have same leader: %s" run (show leaders)) $ hasCommon leaders
    assertBool (printf "%d: There must be a leader %s" run (show leaders)) $ all (/= Nothing) leaders
    let leader = (leaders !! 0)
    case possibleLeader of
        Just _ -> if all (== possibleLeader) leaders
                            then return leader
                            else return Nothing
        Nothing -> if all (== leader) leaders
                        then return leader
                        else return Nothing

waitForLeader :: Integer -> Integer -> [Raft IntLog IntCommand IntState] -> IO (Maybe Name)
waitForLeader maxCount attempt vRafts = do
    leaders <- allLeaders vRafts
    let leader = leaders !! 0
    if maxCount <= 0
        then do
            assertBool ("No leader found after " ++ (show (attempt - 1)) ++ " rounds: " ++ (show leaders)) False
            return Nothing
        else do
            pause
            if (leader /= Nothing) && (all (== leader) leaders)
                then do
                    let msg = "After " ++ (show attempt) ++ " rounds, the leader is " ++ (show leader)
                    if attempt > 3
                        then warningM _log msg
                        else infoM _log msg
                    return leader
                else
                    waitForLeader (maxCount - 1) (attempt + 1) vRafts

allLeaders :: [Raft IntLog IntCommand IntState] -> IO [Maybe Name]
allLeaders vRafts = do
    rafts <- mapM (\vRaft -> atomically $ readTVar $ raftContext vRaft) vRafts
    let leaders = map (clusterLeader . clusterConfiguration . raftStateConfiguration . raftState) rafts
    return leaders

allStates :: [Raft IntLog IntCommand IntState] -> IO [IntState]
allStates vRafts = do
    rafts <- mapM (\vRaft -> atomically $ readTVar $ raftContext vRaft) vRafts
    let states = map (raftStateData . raftState) rafts
    return states

allLastCommitted :: [Raft IntLog IntCommand IntState] -> IO [Index]
allLastCommitted vRafts = do
    rafts <- mapM (\vRaft -> atomically $ readTVar $ raftContext vRaft) vRafts
    let committed = map (lastCommitted . raftLog) rafts
    return committed

withClient :: Transport -> Name -> RaftConfiguration -> (Client -> IO a) -> IO a
withClient transport name cfg fn = do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    let client = newClient endpoint name cfg
    finally (fn client)
        (unbindEndpoint_ endpoint name)

hasCommon :: (Eq a,Ord a) => [a] -> Bool
hasCommon values = let (_,count) = common values
                       majority = 1 + ((length values ) `quot` 2)
                      in count >= majority 

common :: (Eq a,Ord a) => [a] -> (a,Int)
common values = classify values M.empty
    where
        classify [] classified = mostCommon $ M.toList classified
        classify (value:rest) classified  = classify rest (tally value classified)
        mostCommon [] = error "No common values!"
        mostCommon (pair:[]) = pair
        mostCommon (pair:rest) = let (value,count) = pair
                                     (highestValue,highestCount) = mostCommon rest
                                    in if count > highestCount 
                                        then (value,count)
                                        else (highestValue,highestCount)
        tally value classified = case M.lookup value classified of
            Just count -> M.insert value (count + 1) classified
            Nothing -> M.insert value 1 classified

newTestConfiguration :: [Name] -> RaftConfiguration
newTestConfiguration members = (mkRaftConfiguration members) {clusterTimeouts = testTimeouts}

newSocketTestConfiguration :: [Name] -> RaftConfiguration
newSocketTestConfiguration members = (mkRaftConfiguration members) {clusterTimeouts = testSocketTimeouts}

pause :: IO ()
pause = threadDelay serverTimeout

testTimeouts :: Timeouts
testTimeouts = timeouts (25 * 1000)

testSocketTimeouts :: Timeouts
testSocketTimeouts = timeouts (150 * 1000)

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
withServer :: Transport -> RaftConfiguration -> Name -> (IntRaft -> IO ()) -> IO ()
withServer transport cfg name fn = do
    endpoint <- newEndpoint [transport]
    bindEndpoint_ endpoint name
    initialLog <- mkIntLog
    let initialState = mkRaftState (IntState 0) cfg name
    finally (withConsensus endpoint name initialLog initialState fn)
        (unbindEndpoint_ endpoint name)

with3Servers :: Transport -> RaftConfiguration -> ([IntRaft] -> IO ()) -> IO ()
with3Servers transport cfg fn = 
    let names = clusterMembers $ clusterConfiguration cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 -> fn $ [vRaft1,vRaft2,vRaft3]

with3Servers1NonParticipant :: Transport -> RaftConfiguration -> Name -> ([IntRaft] -> IO ()) -> IO ()
with3Servers1NonParticipant transport cfg name fn = 
    let names = clusterMembers $ clusterConfiguration cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 -> 
        withServer transport cfg name $ \vRaft4 -> fn $ [vRaft1, vRaft2, vRaft3, vRaft4]

with5Servers :: Transport -> RaftConfiguration -> ([IntRaft] -> IO ()) -> IO ()
with5Servers transport cfg fn = 
    let names = clusterMembers $ clusterConfiguration cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 ->
        withServer transport cfg (names !! 3) $ \vRaft4 ->
        withServer transport cfg (names !! 4) $ \vRaft5 -> fn $ [vRaft1] ++ [vRaft2] ++ [vRaft3] ++ [vRaft4] ++ [vRaft5]

with7Servers :: Transport -> RaftConfiguration -> ([IntRaft] -> IO ()) -> IO ()
with7Servers transport cfg fn = 
    let names = clusterMembers $ clusterConfiguration cfg
    in withServer transport cfg (names !! 0) $ \vRaft1 ->
        withServer transport cfg (names !! 1) $ \vRaft2 ->
        withServer transport cfg (names !! 2) $ \vRaft3 ->
        withServer transport cfg (names !! 3) $ \vRaft4 ->
        withServer transport cfg (names !! 4) $ \vRaft5 ->
        withServer transport cfg (names !! 5) $ \vRaft6 ->
        withServer transport cfg (names !! 6) $ \vRaft7 -> fn $ [vRaft1] ++ [vRaft2] ++ [vRaft3] ++ [vRaft4] ++ [vRaft5] ++ [vRaft6] ++ [vRaft7]
