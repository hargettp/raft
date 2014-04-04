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
    withConsensus
) where

-- local imports

import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Protocol
import Control.Consensus.Raft.State
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

import System.CPUTime
import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
withConsensus :: (RaftLog l v) => Endpoint -> RaftServer l v -> (Raft l v -> IO ()) -> IO ()
withConsensus endpoint server fn = do
    vRaft <- atomically $ newRaft server
    withAsync (run vRaft)
        (\_ -> fn vRaft)
    where
        run vRaft = do
            infoM _log $ "Starting server " ++ (serverId server)
            finally (catch (participate vRaft)
                        (\e -> case e of
                                ThreadKilled -> return ()
                                _ -> debugM _log $ (show $ serverId server) ++ " encountered error: " ++ (show (e :: AsyncException))))
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
                doVote vRaft endpoint name False,
                doRedirect vRaft endpoint name]

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> Bool -> IO ()
doFollow vRaft endpoint member leading = do
    -- TOD what if we're the leader and we're processing a message
    -- we sent earlier?
    cfg <- atomically $ do
        raft <- readTVar vRaft
        return $ serverConfiguration $ serverState $ raftServer raft
    maybeCommitted <- onAppendEntries endpoint cfg member $ \req -> do
        debugM _log $ "Server " ++ member ++ " received " ++ (show req)
        infoM _log $ "Server " ++ member ++ " received pulse from " ++ (aeLeader req)
        term <- do
            raft <- atomically $ readTVar vRaft
            raftLastLogEntryTerm $ serverLog $ raftServer raft
        -- grab these entries, because we can't do so inside the transaction
        atomically $ do
            raft <-readTVar vRaft
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return $ createResult False raft
                else do
                    -- first update term and leader
                    if (aeLeaderTerm req) > (raftCurrentTerm raft)
                        then  modifyTVar vRaft $ \oldRaft ->
                            setRaftTerm (aeLeaderTerm req)
                                $ setRaftLeader (Just $ aeLeader req)
                                $ setRaftLastCandidate Nothing oldRaft
                        else if ((aeLeaderTerm req) == (raftCurrentTerm raft)) &&
                                    Nothing == (clusterLeader $ serverConfiguration $ serverState $ raftServer raft)
                                then modifyTVar vRaft $ \oldRaft ->
                                    setRaftLeader (Just $ aeLeader req)
                                        $ setRaftLastCandidate Nothing oldRaft
                                else return ()
                    -- now check that we're in sync
                    return $ createResult (term == (aePreviousTerm req)) raft
    case maybeCommitted of
        Just committed ->  do
            -- what is good here is that since there is only 1 doFollow
            -- async, we can count on all of these invocations to commit as
            -- being synchronous, so no additional locking required
            raft <- atomically $ readTVar vRaft
            (log,state) <- commitEntries (serverLog $ raftServer $ raft) committed (serverState $ raftServer $ raft)
            atomically $ modifyTVar vRaft $ \oldRaft ->
                        setRaftLog log $ setRaftServerState state oldRaft
            if leading
                then return ()
                else doFollow vRaft endpoint member leading
        Nothing -> if leading
            then doFollow vRaft endpoint member leading
            else return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> Bool -> IO ()
doVote vRaft endpoint name leading = do
    log <- atomically $ do
        raft <- readTVar vRaft
        return $ serverLog $ raftServer raft
    logLastEntryIndex <- raftLastLogEntryIndex log
    logLastEntryTerm <- raftLastLogEntryTerm log
    won <- onRequestVote endpoint name $ \req -> do
        debugM _log $ "Server " ++ name ++ " received vote request from " ++ (show $ rvCandidate req)
        (raft,vote,reason) <- atomically $ do
            raft <- readTVar vRaft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raft,False,"Candidate term too old")
                else do
                    modifyTVar vRaft $ \oldRaft -> 
                            let newTerm = max (raftCurrentTerm oldRaft) (rvCandidateTerm req)
                            in setRaftTerm newTerm oldRaft
                    case raftLastCandidate raft of
                        Just candidate -> do
                            if (candidate == rvCandidate req)
                                then return (raft,True,"Candidate already seen")
                                else return (raft,False,"Already saw different candidate")
                        Nothing -> do
                            modifyTVar vRaft $ \oldRaft -> 
                                    setRaftLastCandidate (Just $ rvCandidate req) oldRaft
                            if name == rvCandidate req
                                then return (raft,True,"Voting for self")
                                else do
                                    if candidateMoreUpToDate req logLastEntryTerm logLastEntryIndex
                                        then do
                                            modifyTVar vRaft $ \oldRaft -> 
                                                setRaftLastCandidate (Just $ rvCandidate req) oldRaft
                                            return (raft,True,"Candidate log more up to date")
                                        else return (raft,False,"Candidate log out of date")
        infoM _log $ "Server " ++ name ++ " vote for " ++ rvCandidate req ++ " is " 
            ++ (show (raftCurrentTerm raft,vote)) ++ " because " ++ reason
        return $ createResult vote raft
    if leading && won
        then return ()
        else doVote vRaft endpoint name leading
    where
        -- check that candidate log is more up to date than this server's log
        candidateMoreUpToDate req term index= if (rvCandidateLastEntryTerm req) > term
            then True
            else if (rvCandidateLastEntryTerm req) < term
                    then False
                    else (rvCandidateLastEntryIndex req) >= index

{-|
Initiate an election, volunteering to lead the cluster if elected.
-}
volunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO Bool
volunteer vRaft endpoint name = do
    results <- race (doVote vRaft endpoint name False) (doVolunteer vRaft endpoint name)
    case results of
        Left _ -> return False
        Right won -> return won

doVolunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO Bool
doVolunteer vRaft endpoint candidate = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    infoM _log $ "Server " ++ candidate ++ " volunteering in term " ++ (show $ raftCurrentTerm raft)
    let cfg = serverConfiguration $ serverState $ raftServer raft
        members = clusterMembers cfg
        cs = newCallSite endpoint candidate
        term = raftCurrentTerm raft
        log = serverLog $ raftServer raft
        lastIndex = lastAppended log
    lastTerm <- raftLastLogEntryTerm log
    debugM _log $ "Server " ++ candidate ++ " is soliciting votes from " ++ (show members)
    votes <- goRequestVote cs cfg term candidate lastIndex lastTerm
    debugM _log $ "Server " ++ candidate ++ " (" ++ (show (term,lastIndex)) ++ ") received votes " ++ (show votes)
    return $ wonElection votes
    where
        wonElection :: M.Map Name (Maybe MemberResult) -> Bool
        wonElection votes = majority votes $ M.foldl (\tally ballot -> 
            case ballot of
                Just (result) -> if (memberActionSuccess result) then tally + 1 else tally 
                _ -> tally) 0 votes
        majority :: M.Map Name (Maybe MemberResult) -> Int -> Bool
        majority votes tally = tally > ((M.size $ votes) `quot` 2)

lead :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> ServerId -> IO ()
lead vRaft endpoint name = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let cfg = serverConfiguration $ serverState $ raftServer raft
        members = clusterMembersOnly cfg
        leader = name
        term = raftCurrentTerm raft
    infoM _log $ "Server " ++ name ++ " leading in new term " ++ (show term)
    followers <- mapM (makeFollower term) members
    raceAll_ $ [doPulse vRaft leader followers,
                     doVote vRaft endpoint leader True,
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
                cfg = serverConfiguration $ serverState server
                leader = serverId server
                log = serverLog server
                appended = lastAppended log
                cs = newCallSite endpoint leader
                prevLogIndex = index - 1
            previousEntries <- fetchEntries log prevLogIndex 1
            entries <- fetchEntries log index $ appended - index + 1
            infoM _log $ "Server " ++ leader ++ " notifying member " ++ member ++ " in term " ++ (show term)
            start <- getCPUTime
            response <- case previousEntries of
                [] -> goAppendEntries cs cfg member term prevLogIndex (-1) index entries
                _ -> let prevTerm = entryTerm $ last previousEntries
                                     in goAppendEntries cs cfg member term prevLogIndex prevTerm index entries
            stop <- getCPUTime
            let diff =  quot (stop - start) 1000000 -- 1 million picoseconds in a microsecond
            if diff > (toInteger $ timeoutRpc $ configurationTimeouts cfg)
                then warningM _log $ "Elapsed time longer than rpcTimeout from " ++ leader ++ " to " ++ member ++ ": " ++ (show diff)
                else return ()
            case response of
                Nothing -> notify term lastIndex member
                Just result -> if (memberCurrentTerm result) > term
                        then do
                            -- we have seen a member with a higher term, so we have to change terms
                            -- and step down; but make sure we update ourselves to the highest known term
                            atomically $ modifyTVar vRaft $ \oldRaft ->
                                setRaftTerm (foldl max term [(memberCurrentTerm result),(raftCurrentTerm oldRaft)]) 
                                    $ setRaftLastCandidate Nothing oldRaft
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
    let cfg = serverConfiguration $ serverState $ raftServer raft
    if Just name == (clusterLeader cfg)
        then do
            let index = lastCommitted $ serverLog $ raftServer raft
            mapM_ (pulse index) followers
            threadDelay $ timeoutPulse $ configurationTimeouts $ cfg
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
        atomically $ modifyTVar vRaft $ \oldRaft -> setRaftLog rlog1 oldRaft
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
