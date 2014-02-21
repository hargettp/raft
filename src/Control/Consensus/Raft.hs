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
runConsensus :: (RaftLog l v) => Endpoint -> RaftServer l v -> IO (RaftServer l v)
runConsensus endpoint server = do
    vRaft <- atomically $ newTVar $ RaftState {
        raftCurrentTerm = 0,
        raftLastCandidate = Nothing,
        raftServer = server
    }
    catch (run vRaft) (\e -> do
                case e of
                    ThreadKilled -> return ()
                    _ -> debugM _log $ (show $ serverId server) ++ " encountered error: " ++ (show (e :: AsyncException)))
    raft <- atomically $ readTVar vRaft
    return $ raftServer raft
    where
        run vRaft = do
            infoM _log $ "Starting server " ++ (serverId server)
            finally (do participate vRaft)
                (do
                    infoM _log $ "Stopped server " ++ (serverId server) )
        participate vRaft = do
            follow vRaft endpoint $ serverId server
            won <- volunteer vRaft endpoint $ serverId server
            if won
                then do
                    lead vRaft endpoint $ serverId server
                else return ()
            participate vRaft

follow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
follow vRaft endpoint name = do
    term <- atomically $ do
        raft <- readTVar vRaft
        return $ raftCurrentTerm raft
    infoM _log $ "Server " ++ name ++ " following " ++ " in term " ++ (show term)
    race_ (doFollow vRaft endpoint name) (doVote vRaft endpoint name)

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doFollow vRaft endpoint member = do
    (committed,success) <- onAppendEntries endpoint member $ \req -> do
        debugM _log $ "Server " ++ member ++ " received AppendEntries " ++ (show req)
        raft <- atomically $ readTVar vRaft
        -- grab these entries, because we can't do so inside the transaction
        entries <- fetchEntries (serverLog $ raftServer raft) (aePreviousIndex req) 1
        atomically $ if (aeLeaderTerm req) < (raftCurrentTerm raft)
            then return (raftCurrentTerm raft,False)
            else do
                -- first update term and leader
                raft1 <- do
                    if (aeLeaderTerm req) > (raftCurrentTerm raft)
                        then  modifyTVar vRaft $ \oldRaft ->
                            changeRaftTerm (aeLeaderTerm req)
                                $ changeRaftLeader (Just $ aeLeader req)
                                $ changeRaftLastCandidate Nothing oldRaft
                        else if ((aeLeaderTerm req) == (raftCurrentTerm raft)) && 
                                    Nothing == (clusterLeader $ serverConfiguration $ serverState $ raftServer raft)
                            then modifyTVar vRaft $ \oldRaft -> 
                                changeRaftLeader (Just $ aeLeader req)
                                    $ changeRaftLastCandidate Nothing oldRaft
                            else return ()
                    readTVar vRaft
                -- now check that we're in sync
                case entries of
                    -- we can only have an empty list if we are at the beginning of the log
                    [] -> return (raftCurrentTerm raft1,-1 == aePreviousIndex req)
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
        debugM _log $ "Server " ++ name ++ " received vote request from " ++ (show $ rvCandidate req)
        (term,vote,reason) <- atomically $ do
            raft <- readTVar vRaft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raftCurrentTerm raft,False,"Candidate term too old")
                else do
                    case raftLastCandidate raft of
                        Just candidate -> do
                            if (candidate == rvCandidate req)
                                then return (raftCurrentTerm raft,True,"Candidate already seen")
                                else return (raftCurrentTerm raft,False,"Already saw different candidate")
                        Nothing -> do
                            if name == rvCandidate req
                                then return (raftCurrentTerm raft,True,"Voting for self")
                                else if logOutOfDate raft req
                                    then do
                                        modifyTVar vRaft $ \oldRaft -> 
                                            let newTerm = if (raftCurrentTerm oldRaft) < (rvCandidateTerm req)
                                                then rvCandidateTerm req
                                                else (raftCurrentTerm oldRaft)
                                            in changeRaftTerm newTerm
                                                $ changeRaftLastCandidate (Just $ rvCandidate req) oldRaft
                                        return (raftCurrentTerm raft,True,"Candidate log more up to date")
                                    else return (raftCurrentTerm raft,False,"Candidate log out of date")
        -- raft <- atomically $ readTVar vRaft
        debugM _log $ "Server " ++ name ++ " vote for " ++ rvCandidate req ++ " is " ++ (show (term,vote)) ++ " because " ++ reason
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
        modifyTVar vRaft $ \oldRaft ->
            changeRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ changeRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    infoM _log $ "Server " ++ candidate ++ " volunteering in term " ++ (show $ raftCurrentTerm raft)
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
    debugM _log $ "Server " ++ candidate ++ " is soliciting votes from " ++ (show members)
    votes <- goRequestVote cs members term candidate lastIndex lastTerm
    debugM _log $ "Server " ++ candidate ++ " (" ++ (show (term,lastIndex)) ++ ") received votes " ++ (show votes)
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
        modifyTVar vRaft $ \oldRaft ->
            changeRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ changeRaftLeader (Just name)
                $ changeRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let members = clusterMembersOnly $ serverConfiguration $ serverState $ raftServer raft
        leader = name
        term = raftCurrentTerm raft
    infoM _log $ "Server " ++ name ++ " leading in new term " ++ (show term)
    followers <- mapM (makeFollower term) members
    serving <- async $ doServe endpoint leader vRaft followers
    pulsing <- async $ doPulse vRaft leader followers
    voting <- async $ doVote vRaft endpoint leader
    _ <- waitAnyCancel $ [pulsing,serving,voting] ++ map followerNotifier followers
    return ()
    where
        makeFollower term member = do
            lastIndex <- atomically $ newEmptyTMVar
            notifier <- async $ notify term lastIndex member
            return Follower {
                followerLastIndex = lastIndex,
                followerNotifier = notifier
            }
        notify term lastIndex member= do
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
                Nothing -> notify term lastIndex member
                Just (memberTerm,success) -> if memberTerm > term
                        then do
                            -- TODO this doesn't feel atomic enough
                            atomically $ modifyTVar vRaft $ \oldRaft -> oldRaft {raftCurrentTerm = memberTerm}
                            debugM _log $ "Leader " ++ name ++ " has lower term than member " ++ member
                            return ()
                        else if success
                            -- TODO figure these calls out
                            then notify term lastIndex member
                            else notify term lastIndex member
            return ()

doPulse :: (RaftLog l v) => TVar (RaftState l v) -> ServerId -> [Follower] -> IO ()
doPulse vRaft name followers = do
    raft <- atomically $ readTVar vRaft
    if Just name == (clusterLeader $ serverConfiguration $ serverState $ raftServer raft)
        then do
            let index = lastCommitted $ serverLog $ raftServer raft
            mapM_ (pulse index) followers
            threadDelay pulseTimeout
            doPulse vRaft name followers
        else return ()
    where
        pulse index follower = atomically $ do
            _ <- tryPutTMVar (followerLastIndex follower) index
            return ()

doServe :: (RaftLog l v) => Endpoint -> ServerId -> TVar (RaftState l v) -> [Follower] -> IO ()
doServe endpoint leader vRaft followers = do
    -- onPerformAction endpoint leader $ action -> do
    doServe endpoint leader vRaft followers

data Follower = Follower {
    followerLastIndex :: TMVar Index,
    followerNotifier :: Async ()
}
