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

import Control.Consensus.Log

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
    raceAll_ [doFollow vRaft endpoint name False,
                doVote vRaft endpoint name,
                doRedirect vRaft endpoint name]

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> Bool -> IO ()
doFollow vRaft endpoint member leading = do
    (committed,continue) <- onAppendEntries endpoint member $ \req -> do
        debugM _log $ "Server " ++ member ++ " received " ++ (show req)
        raft <- atomically $ readTVar vRaft
        -- grab these entries, because we can't do so inside the transaction
        entries <- fetchEntries (serverLog $ raftServer raft) (aePreviousIndex req) 1
        atomically $ if (aeLeaderTerm req) < (raftCurrentTerm raft)
            then return $ createResult False raft
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
                    [] -> return $ createResult (-1 == aePreviousIndex req) raft1
                    (entry:_) -> let term = raftCurrentTerm raft1
                                     in return $ createResult (term == (entryTerm entry)) raft1
    if continue
        then do
            -- what is good here is that since there is only 1 doFollow
            -- async, we can count on all of these invocations to commit as
            -- being synchronous, so no additional locking required
            raft <- atomically $ readTVar vRaft
            (log,state) <- commitEntries (serverLog $ raftServer $ raft) committed (serverState $ raftServer $ raft)
            atomically $ modifyTVar vRaft $ \oldRaft ->
                        let oldServer = raftServer oldRaft
                            in oldRaft {raftServer = oldServer {serverLog = log,serverState = state} }
            if leading
                then return ()
                else doFollow vRaft endpoint member leading
        else return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doVote vRaft endpoint name = do
    castVote vRaft endpoint name
    doVote vRaft endpoint name

{-
A leader votes, but also resigns, because another election implies other cluster
members have lost touch with the leader.
-}
doResign :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doResign = castVote

castVote :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
castVote vRaft endpoint name = 
    onRequestVote endpoint name $ \req -> do
        debugM _log $ "Server " ++ name ++ " received vote request from " ++ (show $ rvCandidate req)
        (raft,vote,reason) <- atomically $ do
            raft <- readTVar vRaft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raft,False,"Candidate term too old")
                else do
                    case raftLastCandidate raft of
                        Just candidate -> do
                            if (candidate == rvCandidate req)
                                then return (raft,True,"Candidate already seen")
                                else return (raft,False,"Already saw different candidate")
                        Nothing -> do
                            if name == rvCandidate req
                                then return (raft,True,"Voting for self")
                                else if logOutOfDate raft req
                                    then do
                                        modifyTVar vRaft $ \oldRaft -> 
                                            let newTerm = if (raftCurrentTerm oldRaft) < (rvCandidateTerm req)
                                                            then rvCandidateTerm req
                                                            else (raftCurrentTerm oldRaft)
                                            in changeRaftTerm newTerm
                                                $ changeRaftLastCandidate (Just $ rvCandidate req) oldRaft
                                        return (raft,True,"Candidate log more up to date")
                                    else return (raft,False,"Candidate log out of date")
        debugM _log $ "Server " ++ name ++ " vote for " ++ rvCandidate req ++ " is " ++ (show (raftCurrentTerm raft,vote)) ++ " because " ++ reason
        return $ createResult vote raft
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
        wonElection :: M.Map Name (Maybe MemberResult) -> Bool
        wonElection votes = majority votes $ M.foldl (\tally ballot -> 
            case ballot of
                Just (result) -> if (memberActionSuccess result) then tally + 1 else tally 
                _ -> tally
            )
            0 votes
        majority :: M.Map Name (Maybe MemberResult) -> Int -> Bool
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
    raceAll_ $ [doPulse vRaft leader followers,
                     doResign vRaft endpoint leader,
                     doFollow vRaft endpoint name True,
                     doServe vRaft endpoint leader followers]
                ++ map followerNotifier followers
    where
        makeFollower term member = do
            lastIndex <- atomically $ newEmptyTMVar
            return Follower {
                followerLastIndex = lastIndex,
                followerNotifier = notify term lastIndex member
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
                Just result -> if (memberCurrentTerm result) > term
                        then do
                            -- TODO this doesn't feel atomic enough
                            atomically $ modifyTVar vRaft $ \oldRaft -> oldRaft {raftCurrentTerm = memberCurrentTerm result}
                            debugM _log $ "Leader " ++ name ++ " has lower term than member " ++ member
                            return ()
                        else if (memberActionSuccess result)
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

doServe :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> [Follower] -> IO ()
doServe vRaft endpoint leader followers = do
    infoM _log $ "Serving from " ++ leader
    onPerformAction endpoint leader $ \action -> do
        infoM _log $ "Leader " ++ leader ++ " received action " ++ (show action)
        raft <- atomically $ readTVar vRaft
        let rlog = serverLog $ raftServer raft
            term = raftCurrentTerm raft
            nextIndex = (lastAppended rlog) + 1
        rlog1 <- appendEntries rlog nextIndex [RaftLogEntry term action]
        atomically $ modifyTVar vRaft $ \oldRaft -> changeRaftLog rlog1 oldRaft
        return $ createResult True raft
    doServe vRaft endpoint leader followers

doRedirect :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
doRedirect vRaft endpoint member = do
    term <- atomically $ do
        raft <- readTVar vRaft
        return $ raftCurrentTerm raft
    infoM _log $ "Redirecting from " ++ member ++ " in " ++ (show term)
    onPerformAction endpoint member $ \action -> do
        infoM _log $ "Member " ++ member ++ " received action " ++ (show action)
        raft <- atomically $ readTVar vRaft
        return $ createResult False raft
    doRedirect vRaft endpoint member

data Follower = Follower {
    followerLastIndex :: TMVar Index,
    followerNotifier :: IO ()
}

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
