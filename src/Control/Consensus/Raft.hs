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
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Members
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
withConsensus :: (RaftLog l v) => Endpoint -> RaftServer l v -> (Raft l v -> IO ()) -> IO ()
withConsensus endpoint server fn = do
    vRaft <- atomically $ mkRaft server
    withAsync (run vRaft)
        (\_ -> fn vRaft)
    where
        run vRaft = do
            infoM _log $ "Starting server " ++ (serverName server)
            finally (catch (participate vRaft)
                        (\e -> case e of
                                ThreadKilled -> return ()
                                _ -> debugM _log $ (show $ serverName server) ++ " encountered error: " ++ (show (e :: AsyncException))))
                (do
                    infoM _log $ "Stopped server " ++ (serverName server) )
        participate vRaft = do
            follow vRaft endpoint $ serverName server
            won <- volunteer vRaft endpoint $ serverName server
            if won
                then do
                    lead vRaft endpoint $ serverName server
                else return ()
            participate vRaft

follow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> IO ()
follow vRaft endpoint name = do
    term <- atomically $ do
        modifyTVar vRaft $ \oldRaft -> setRaftLeader Nothing oldRaft
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
doFollow :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> Bool -> IO ()
doFollow vRaft endpoint member leading = do
    -- TOD what if we're the leader and we're processing a message
    -- we sent earlier?
    cfg <- atomically $ do
        raft <- readTVar vRaft
        return $ serverConfiguration $ serverState $ raftServer raft
    maybeCommitted <- onAppendEntries endpoint cfg member $ \req -> do
        debugM _log $ "Server " ++ member ++ " received " ++ (show req)
        infoM _log $ "Server " ++ member ++ " received pulse from " ++ (aeLeader req)
        (success, raft) <- atomically $ do
            raft <-readTVar vRaft
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return (False,raft)
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
                    -- we assume terms are equal if we're here
                    return (True,raft)
        if success
            then do
                let log = serverLog $ raftServer raft
                newLog <- appendEntries log (logIndex $ aePreviousTime req) (aeEntries req)
                updatedRaft <- atomically $ do 
                        modifyTVar vRaft $ \oldRaft -> setRaftLog newLog oldRaft
                        readTVar vRaft
                return $ mkResult success updatedRaft
            else return $ mkResult success raft
    case maybeCommitted of
        Just committed ->  do
            -- what is good here is that since there is only 1 doFollow
            -- async, we can count on all of these invocations to commit as
            -- being synchronous, so no additional locking required
            raft <- atomically $ readTVar vRaft
            (log,state) <- commitEntries (serverLog $ raftServer $ raft) (logIndex $ committed) (serverState $ raftServer $ raft)
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
doVote :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> Bool -> IO ()
doVote vRaft endpoint name leading = do
    log <- atomically $ do
        raft <- readTVar vRaft
        return $ serverLog $ raftServer raft
    let logLastEntryTime = lastAppendedTime log
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
                                    if (rvCandidateLastEntryTime req) >= logLastEntryTime
                                        then do
                                            modifyTVar vRaft $ \oldRaft -> 
                                                setRaftLastCandidate (Just $ rvCandidate req) oldRaft
                                            return (raft,True,"Candidate log more up to date")
                                        else return (raft,False,"Candidate log out of date")
        infoM _log $ "Server " ++ name ++ " vote for " ++ rvCandidate req ++ " is "
            ++ (show (raftCurrentTerm raft,vote)) ++ " because " ++ reason
        return $ mkResult vote raft
    if leading && won
        then return ()
        else doVote vRaft endpoint name leading

{-|
Initiate an election, volunteering to lead the cluster if elected.
-}
volunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> IO Bool
volunteer vRaft endpoint name = do
    results <- race (doVote vRaft endpoint name False) (doVolunteer vRaft endpoint name)
    case results of
        Left _ -> return False
        Right won -> return won

doVolunteer :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> IO Bool
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
        RaftTime lastTerm lastIndex = lastAppendedTime log
    debugM _log $ "Server " ++ candidate ++ " is soliciting votes from " ++ (show members)
    votes <- goRequestVote cs cfg term candidate (RaftTime lastTerm lastIndex)
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

lead :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> IO ()
lead vRaft endpoint name = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let cfg = serverConfiguration $ serverState $ raftServer raft
        members = mkMembers cfg
        leader = name
        term = raftCurrentTerm raft
    clients <- atomically $ newMailbox
    vLatest <- atomically $ newEmptyTMVar
    infoM _log $ "Server " ++ name ++ " leading in term " ++ (show term)
    raceAll_ $ [doPulse vRaft vLatest,
                doVote vRaft endpoint leader True,
                doFollow vRaft endpoint leader True,
                doServe vRaft endpoint leader clients vLatest,
                doCommit vRaft endpoint leader members clients vLatest]

doPulse :: (RaftLog l v) => TVar (RaftState l v) -> TMVar Index -> IO ()
doPulse vRaft vLatest = do
    cfg <- atomically $ do
        raft <- readTVar vRaft
        let index = lastAppended $ serverLog $ raftServer raft
        empty <- isEmptyTMVar vLatest
        if empty
            then putTMVar vLatest index
            else return ()
        return $ serverConfiguration $ serverState $ raftServer raft
    threadDelay $ timeoutPulse $ configurationTimeouts cfg
    doPulse vRaft vLatest

doServe :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> Mailbox (Index,Reply MemberResult) -> TMVar Index -> IO ()
doServe vRaft endpoint leader clients vLatest = do
    (atomically $ readTVar vRaft) >>= \raft -> infoM _log $ "Serving from " ++ leader ++ " in term " ++ (show $ raftCurrentTerm raft)
    onPerformAction endpoint leader $ \action reply -> do
        infoM _log $ "Leader " ++ leader ++ " received action " ++ (show action)
        raft <- atomically $ readTVar vRaft
        let rlog = serverLog $ raftServer raft
            term = raftCurrentTerm raft
            nextIndex = (let RaftTime _ i = lastAppendedTime rlog in i) + 1
        -- TODO this should be changed to append the entries, capture the last appended index,
        -- and put the client on the pending mailbox, broadcast entries to all members.
        -- the leader's commit task will commit once enough members have appended.
        newLog <- appendEntries rlog nextIndex [RaftLogEntry term action]
        atomically $ do
            modifyTVar vRaft $ \oldRaft -> setRaftLog newLog oldRaft
            writeMailbox clients (logIndex $ lastAppendedTime newLog,reply)
        infoM _log $ "Appended action at index " ++ (show $ logIndex $ lastAppendedTime newLog)
    doServe vRaft endpoint leader clients vLatest

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doCommit :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> M.Map Name Member -> Mailbox (Index,Reply MemberResult) -> TMVar Index -> IO ()
doCommit vRaft endpoint leader members pending vLatest = do
    raft <- atomically $ do
        -- interestingly, we don't care about its value at the moment
        _ <- takeTMVar vLatest
        readTVar vRaft
    let rlog = serverLog $ raftServer raft
        cs = newCallSite endpoint leader
        term = raftCurrentTerm raft
        prevTime = lastCommittedTime rlog
        cfg = serverConfiguration $ serverState $ raftServer raft
    (commitIndex,entries) <- fetchLatestEntries rlog
    infoM _log $ "Synchronizing from " ++ leader ++ " in term " ++ (show $ raftCurrentTerm raft) ++ ": " ++ (show entries)
    results <- goSynchronizeEntries cs cfg term prevTime (RaftTime term commitIndex) entries
    infoM _log $ "Synchronized from " ++ leader ++ " in term " ++ (show $ raftCurrentTerm raft)
    let newMembers = updateMembers members results
        newAppendedIndex = membersSafeAppendedIndex newMembers
        newTerm = membersHighestTerm newMembers
    infoM _log $ "Safe appended index is " ++ (show newAppendedIndex) ++ ": " ++ (show $ membersAppendedIndex members)
    if newTerm > (raftCurrentTerm raft)
        then do
            infoM _log $ "Leading stepping down; new term " ++ (show newTerm) ++ " discovered"
            atomically $ modifyTVar vRaft $ \oldRaft ->
                setRaftTerm newTerm oldRaft
            return ()
        else do
            (newLog,newState) <- let time = (RaftTime (raftCurrentTerm raft) newAppendedIndex)
                                     oldCommitedIndex = commitIndex
                                     count = newAppendedIndex - oldCommitedIndex
                                     in if count > 0
                                        then do
                                            infoM _log $ "Committing at time " ++ (show time)
                                            commitEntries rlog newAppendedIndex (serverState $ raftServer $ raft)
                                        else return (rlog,(serverState $ raftServer $ raft))
            maybeReply <- atomically $ do
                modifyTVar vRaft $ \oldRaft ->
                    setRaftLog newLog $ setRaftServerState newState oldRaft
                maybeClient <- tryPeekMailbox pending
                case maybeClient of
                    Nothing -> return Nothing
                    Just (index,reply) ->
                        if index <= (logIndex $ lastCommittedTime newLog)
                            then do
                                updatedRaft <- readTVar vRaft
                                _ <- readMailbox pending
                                return $ Just (index, reply $ mkResult True updatedRaft)
                            else return Nothing
            case maybeReply of
                Nothing -> do
                    infoM _log $ "No client at index " ++ (show $ logIndex $ lastCommittedTime newLog)
                    return ()
                Just (index,reply) -> do
                    infoM _log $ "Replying at index " ++ (show index)
                    reply
                    infoM _log $ "Replied at index " ++ (show index)
            doCommit vRaft endpoint leader newMembers pending vLatest

doRedirect :: (RaftLog l v) => TVar (RaftState l v) -> Endpoint -> Name -> IO ()
doRedirect vRaft endpoint member = do
    term <- atomically $ do
        raft <- readTVar vRaft
        return $ raftCurrentTerm raft
    infoM _log $ "Redirecting from " ++ member ++ " in " ++ (show term)
    onPerformAction endpoint member $ \action reply -> do
        infoM _log $ "Member " ++ member ++ " received action " ++ (show action)
        raft <- atomically $ readTVar vRaft
        reply $ mkResult False raft
    doRedirect vRaft endpoint member

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
