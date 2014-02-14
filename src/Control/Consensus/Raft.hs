{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft
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

module Control.Consensus.Raft (
    runConsensus
) where

-- local imports

import Control.Consensus.Raft.Protocol
import Control.Consensus.Raft.Types

import Data.Log

-- external imports

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Concurrent.STM

import qualified Data.Map as M

import Network.Endpoints
import Network.RPC

import Prelude hiding (log)

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
runConsensus :: (RaftLog l v) => Endpoint -> RaftServer l v -> IO ()
runConsensus endpoint server = do
  catch run (\e -> do
                errorM _log $ (show $ serverId server) ++ " encountered error: " ++ (show (e :: SomeException)))
  where
    run = do
        raft <- atomically $ newTVar $ RaftState {
            raftCurrentTerm = 0,
            raftLastCandidate = Nothing,
            raftServer = server
        }
        infoM _log $ "Starting server " ++ (serverId server)
        finally (do participate raft)
            (do
                infoM _log $ "Stopped server " ++ (serverId server) )
    participate raft = do
        infoM _log $ "Server " ++ (serverId server) ++ " following"
        follow raft endpoint $ serverId server
        infoM _log $ "Server " ++ (serverId server) ++ " volunteering"
        won <- volunteer raft endpoint $ serverId server
        if won
            then do
                infoM _log $ "Server " ++ (serverId server) ++ " leading"
                lead raft endpoint $ serverId server
            else return ()
        participate raft

follow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
follow vRaft endpoint name = race_ (doFollow vRaft endpoint name) (doVote vRaft endpoint name)

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doFollow vRaft endpoint member = do
    (committed,success) <- onAppendEntries endpoint member $ \req -> do
        raft <- atomically $ readTVar vRaft
        -- grab these entries, because we can't do so inside the transaction
        entries <- fetchEntries (serverLog $ raftServer raft) (aePreviousIndex req) 1
        atomically $ if (aeLeaderTerm req) < (raftCurrentTerm raft)
            then return (raftCurrentTerm raft,False)
            else do
                -- first update term and leader
                raft1 <- do
                    if (aeLeaderTerm req) > (raftCurrentTerm raft)
                        then  modifyTVar vRaft $ \oldRaft -> oldRaft {
                            raftCurrentTerm = aeLeaderTerm req,
                            raftServer = (raftServer oldRaft) {
                                serverState = (serverState $ raftServer oldRaft) {
                                    serverConfiguration = (serverConfiguration $ serverState $ raftServer oldRaft) {
                                        configurationLeader = Just $ aeLeader req
                                    }}},
                            raftLastCandidate = Nothing}
                        else return ()
                    readTVar vRaft
                -- now check that we're in sync
                case entries of
                    [] -> return (raftCurrentTerm raft1,False)
                    (entry:_) -> let term = raftCurrentTerm raft1
                                     in return (term,(term == (entryTerm entry)))
    if success
        then do
            -- what is good here is that since there is only 1 doFollow
            -- async, we can count on all of these invocations to commit as
            -- being synchronous, so no additional locking required
            raft <- atomically $ readTVar vRaft
            (log,state) <- commitEntries (serverLog $ raftServer $ raft) committed (serverState $ raftServer $ raft)
            atomically $ modifyTVar vRaft $ \oldRaft -> 
                        let oldServer = raftServer oldRaft
                            in oldRaft {raftServer = oldServer {serverLog = log,serverState = state} }
            doFollow vRaft endpoint member
        else return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doVote vRaft endpoint name = do
    onRequestVote endpoint name $ \req -> do
        -- infoM _log $ "Server " ++ name ++ " received vote request from " ++ (show $ rvCandidate req)
        (term,vote) <- atomically $ do
            raft <- readTVar vRaft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raftCurrentTerm raft,False)
                else do
                    raft1 <- readTVar vRaft
                    case raftLastCandidate raft1 of
                        Just candidate -> do
                            if candidate == rvCandidate req
                                then return (raftCurrentTerm raft1,True)
                                else return (raftCurrentTerm raft1,False)
                        Nothing -> do
                            if logOutOfDate raft1 req
                                then do
                                    modifyTVar vRaft $ \oldRaft -> oldRaft {
                                        raftCurrentTerm = if (raftCurrentTerm oldRaft) < (rvCandidateTerm req)
                                            then rvCandidateTerm req
                                            else (raftCurrentTerm oldRaft),
                                        raftLastCandidate = Just $ rvCandidate req}
                                    return (raftCurrentTerm raft1,True)
                                else return (raftCurrentTerm raft1,False)
        -- raft <- atomically $ readTVar vRaft
        -- infoM _log $ "Server " ++ name ++ "(" ++ (show $ raftCurrentTerm raft) ++ ") vote for " ++ rvCandidate req ++ " is " ++ (show (term,vote))
        return (term,vote)
    doVote vRaft endpoint name
    where
        -- check that candidate log is more up to date than this server's log
        logOutOfDate raft req = if (rvCandidateLastEntryTerm req) > (raftCurrentTerm raft)
                                then True
                                else if (rvCandidateLastEntryTerm req) < (raftCurrentTerm raft)
                                    then False
                                    else (lastAppended $ serverLog $ raftServer raft) <= (rvCandidateLastEntryIndex req)

{-|
Initiate an election, volunteering to lead the cluster if elected.
-}
volunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO Bool
volunteer vRaft endpoint name = do
    results <- race (doVote vRaft endpoint name) (doVolunteer vRaft endpoint name)
    case results of
        Left _ -> return False
        Right won -> return won

doVolunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO Bool
doVolunteer vRaft endpoint candidate = do
    raft <- atomically $ do
        modifyTVar vRaft $ \raft -> raft {
            raftCurrentTerm = (raftCurrentTerm raft) + 1,
            raftLastCandidate = Nothing}
        readTVar vRaft
    let members = clusterMembers $ serverConfiguration $ serverState $ raftServer raft
        cs = newCallSite endpoint candidate
        term = raftCurrentTerm raft
        log = serverLog $ raftServer raft
        lastIndex = lastAppended log
    lastEntries <- fetchEntries log lastIndex 1
    let lastTerm = case lastEntries of
            (entry:[]) -> entryTerm entry
            (_:entries) -> entryTerm $ last entries
            _ -> 0
    -- infoM _log $ "Server " ++ candidate ++ " is soliciting votes from " ++ (show members)
    votes <- goRequestVote cs members term candidate lastIndex lastTerm
    infoM _log $ "Server " ++ candidate ++ " received votes " ++ (show votes)
    return $ wonElection votes
    where
        wonElection :: M.Map Name (Maybe (Term,Bool)) -> Bool
        wonElection votes = majority votes $ M.foldl (\tally ballot -> 
            case ballot of
                Just (_,vote) -> if vote then tally + 1 else tally 
                _ -> tally
            )
            0 votes
        majority :: M.Map Name (Maybe (Term,Bool)) -> Int -> Bool
        majority votes tally = tally > ((M.size $ votes) `quot` 2)

lead :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
lead vRaft endpoint name = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft -> oldRaft {
            raftCurrentTerm = (raftCurrentTerm oldRaft) + 1,
            raftServer = (raftServer oldRaft) {
                serverState = (serverState $ raftServer oldRaft) {
                    serverConfiguration = (serverConfiguration $ serverState $ raftServer oldRaft) {
                        configurationLeader = Just name
                    }}},
            raftLastCandidate = Nothing}
        readTVar vRaft
    let members = clusterMembersOnly $ serverConfiguration $ serverState $ raftServer raft
        leader = name
        nextIndex = (lastCommitted $ serverLog $ raftServer raft) + 1
        term = raftCurrentTerm raft
    followers <- mapM (makeFollower term nextIndex) members
    serving <- async $ doServe endpoint leader vRaft followers
    pulsing <- async $ doPulse vRaft followers
    voting <- async $ doVote vRaft endpoint leader
    _ <- waitAnyCancel $ [serving,pulsing,voting] ++ map followerNotifier followers
    return ()
    where
        makeFollower term nextIndex member = do
            lastIndex <- atomically $ newEmptyTMVar
            notifier <- async $ notify term lastIndex nextIndex member
            return Follower {
                followerLastIndex = lastIndex,
                followerNotifier = notifier
            }
        notify term lastIndex nextIndex member= do
            index <- atomically $ takeTMVar lastIndex
            raft <- atomically $ readTVar vRaft
            let server = raftServer raft
                leader = serverId server
                log = serverLog server
                appended = lastAppended log
                cs = newCallSite endpoint leader
                prevLogIndex = index - 1
            previousEntries <- fetchEntries log prevLogIndex 1
            entries <- fetchEntries log index $ appended - index + 1
            response <- case previousEntries of
                [] -> goAppendEntries cs member leader term prevLogIndex (-1) index entries
                _ -> let prevTerm = entryTerm $ last previousEntries
                                     in goAppendEntries cs member leader term prevLogIndex prevTerm index entries
            case response of
                Nothing -> notify term lastIndex nextIndex member
                Just (memberTerm,success) -> if memberTerm > term
                        then return ()
                        else if success
                            then notify term lastIndex (nextIndex + 1) member
                            else notify term lastIndex (nextIndex - 1) member
            return ()

doPulse :: (RaftLog l v) => TVar (RaftState l v) -> [Follower] -> IO ()
doPulse vRaft followers = do
    threadDelay heartbeatTimeout
    raft <- atomically $ readTVar vRaft
    let index = lastCommitted $ serverLog $ raftServer raft
    mapM_ (pulse index) followers
    doPulse vRaft followers
    where
        pulse index follower = atomically $ do
            _ <- tryPutTMVar (followerLastIndex follower) index
            return ()

doServe :: (RaftLog l v) => Endpoint -> ServerId -> TVar (RaftState l v) -> [Follower] -> IO ()
doServe endpoint leader vRaft followers = do
    -- onPerformCommand endpoint leader $ cmd -> do
    doServe endpoint leader vRaft followers

data Follower = Follower {
    followerLastIndex :: TMVar Index,
    followerNotifier :: Async ()
}
