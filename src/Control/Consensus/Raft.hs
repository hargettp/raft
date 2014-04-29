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
-- Generalized implementation of Raft consensus algorithm.
--
-- Application writers generally need only supply implementation of
-- 'Control.Consensus.Log.Log' and 'Control.Consensus.Log.State', together with 
-- defining  'Command's.
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft (
    withConsensus,

    module Control.Consensus.Log,
    module Control.Consensus.Raft.Client,
    module Control.Consensus.Raft.Configuration,
    module Control.Consensus.Raft.Types
) where

-- local imports

import Control.Consensus.Raft.Client
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
import Data.Serialize

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
withConsensus :: (RaftLog l v,Serialize v) => Endpoint -> RaftServer l v -> (Raft l v -> IO ()) -> IO ()
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

follow :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO ()
follow vRaft endpoint name = do
    term <- atomically $ do
        modifyTVar vRaft $ \oldRaft -> setRaftLeader Nothing oldRaft
        raft <- readTVar vRaft
        return $ raftCurrentTerm raft
    infoM _log $ "Server " ++ name ++ " following " ++ " in term " ++ (show term)
    raceAll_ [doFollow vRaft endpoint name False,
                doVote vRaft endpoint name False,
                doRedirect vRaft endpoint name,
                doObserve vRaft endpoint,
                doUnobserve vRaft endpoint name]

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> Endpoint -> Name -> Bool -> IO ()
doFollow vRaft endpoint name leading = do
    -- TOD what if we're the leader and we're processing a message
    -- we sent earlier?
    cfg <- atomically $ do
        raft <- readTVar vRaft
        return $ serverConfiguration $ serverState $ raftServer raft
    maybeCommitted <- onAppendEntries endpoint cfg name $ \req -> do
        debugM _log $ "Server " ++ name ++ " received " ++ (show req)
        infoM _log $ "Server " ++ name ++ " received pulse from " ++ (aeLeader req)
        (valid, raft) <- atomically $ do
            raft <-readTVar vRaft
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return (False,raft)
                else do
                    -- first update raft state
                    modifyTVar vRaft $ \oldRaft ->
                            setRaftTerm (aeLeaderTerm req)
                                $ setRaftLeader (Just $ aeLeader req)
                                $ setRaftLastCandidate Nothing oldRaft
                    let log = serverLog $ raftServer raft
                    -- check previous entry for consistency
                    return ( (lastAppendedTime log) == (aePreviousTime req),raft)
        if valid || not leading
            then do
                let log = serverLog $ raftServer raft
                    index = (1 + (logIndex $ aePreviousTime req))
                newLog <- appendEntries log index (aeEntries req)
                updatedRaft <- atomically $ do
                        modifyTVar vRaft $ \oldRaft -> setRaftLog newLog oldRaft
                        readTVar vRaft
                return $ mkResult valid updatedRaft
            else return $ mkResult valid raft
    case maybeCommitted of
        -- we did hear a message before the timeout, and we have a committed time
        Just (leader,committed) ->  do
            -- what is good here is that since there is only 1 doFollow
            -- async, we can count on all of these invocations to commit as
            -- being synchronous, so no additional locking required
            if not leading
                then do
                    raft <- atomically $ readTVar vRaft
                    (log,state) <- commitEntries (serverLog $ raftServer $ raft) (logIndex $ committed) (serverState $ raftServer $ raft)
                    index <- atomically $ do
                        modifyTVar vRaft $ \oldRaft ->
                                setRaftLog log $ setRaftState state oldRaft
                        return $ lastCommitted log
                    -- notify observers
                    let observers = raftDataObservers raft
                    goNotifyObservers endpoint name observers index $ serverData state
                else return ()
            if leading && (name /= leader)
                then return ()
                else doFollow vRaft endpoint name leading
        -- we heard no message before timeout
        Nothing -> return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> Bool -> IO ()
doVote vRaft endpoint name leading = do
    won <- onRequestVote endpoint name $ \req -> do
        debugM _log $ "Server " ++ name ++ " received vote request from " ++ (show $ rvCandidate req)
        (raft,vote,reason) <- atomically $ do
            raft <- readTVar vRaft
            let log = serverLog $ raftServer raft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raft,False,"Candidate term too old")
                else do
                    modifyTVar vRaft $ \oldRaft -> setRaftTerm (rvCandidateTerm req) oldRaft
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
                                    if (rvCandidateLastEntryTime req) >= (lastAppendedTime log)
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
volunteer :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO Bool
volunteer vRaft endpoint name = do
    results <- race (doVote vRaft endpoint name False) (doVolunteer vRaft endpoint name)
    case results of
        Left _ -> return False
        Right won -> return won

doVolunteer :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO Bool
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

lead :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO ()
lead vRaft endpoint name = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let cfg = serverConfiguration $ serverState $ raftServer raft
        members = mkMembers cfg
        term = raftCurrentTerm raft
    callers <- atomically $ newMailbox
    vLatest <- atomically $ newEmptyTMVar
    infoM _log $ "Server " ++ name ++ " leading in term " ++ (show term)
    raceAll_ $ [doPulse vRaft vLatest,
                doVote vRaft endpoint name True,
                doFollow vRaft endpoint name True,
                doServe vRaft endpoint name callers vLatest,
                doCommit vRaft endpoint name members callers vLatest,
                doObserve vRaft endpoint,
                doUnobserve vRaft endpoint name]

doPulse :: (RaftLog l v) => TVar (RaftContext l v) -> TMVar Index -> IO ()
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

doServe :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> Mailbox (Index,Reply MemberResult) -> TMVar Index -> IO ()
doServe vRaft endpoint leader callers vLatest = do
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
            writeMailbox callers (lastAppended newLog,reply)
        infoM _log $ "Appended action at index " ++ (show $ lastAppended newLog)
    doServe vRaft endpoint leader callers vLatest

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doCommit :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> Endpoint -> Name -> M.Map Name Member -> Mailbox (Index,Reply MemberResult) -> TMVar Index -> IO ()
doCommit vRaft endpoint leader members callers vLatest = do
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
    results <- goAppendEntries cs cfg term prevTime (RaftTime term commitIndex) entries
    infoM _log $ "Synchronized from " ++ leader ++ " in term " ++ (show $ raftCurrentTerm raft)
    let newMembers = updateMembers members results
        newAppendedIndex = membersSafeAppendedIndex newMembers
        newTerm = membersHighestTerm newMembers
    infoM _log $ "Safe appended index is " ++ (show newAppendedIndex) ++ ": " ++ (show newAppendedIndex)
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
            -- update clients
            maybeReply <- atomically $ do
                modifyTVar vRaft $ \oldRaft ->
                    setRaftLog newLog $ setRaftState newState oldRaft
                maybeClient <- tryPeekMailbox callers
                case maybeClient of
                    Nothing -> return Nothing
                    Just (index,reply) ->
                        if index <= (lastCommitted newLog)
                            then do
                                updatedRaft <- readTVar vRaft
                                _ <- readMailbox callers
                                return $ Just (index, reply $ mkResult True updatedRaft)
                            else return Nothing
            case maybeReply of
                Nothing -> do
                    infoM _log $ "No client at index " ++ (show $ lastCommitted newLog)
                    return ()
                Just (index,reply) -> do
                    infoM _log $ "Replying at index " ++ (show index)
                    reply
                    infoM _log $ "Replied at index " ++ (show index)
            -- notify observers
            let observers = raftDataObservers raft
                index = lastCommitted newLog
            goNotifyObservers endpoint leader observers index $ serverData newState
            doCommit vRaft endpoint leader newMembers callers vLatest

doRedirect :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO ()
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

doObserve :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> IO ()
doObserve vRaft endpoint = do
    onObserveData endpoint $ \sub observer -> do
        atomically $ do
            modifyTVar vRaft $ \oldRaft ->
                oldRaft {raftDataObservers = M.insert sub observer (raftDataObservers oldRaft)}
        return ()
    doObserve vRaft endpoint

doUnobserve :: (RaftLog l v) => TVar (RaftContext l v) -> Endpoint -> Name -> IO ()
doUnobserve vRaft endpoint member = do
    onUnobserveData endpoint $ \sub -> do
        atomically $ do
            modifyTVar vRaft $ \oldRaft ->
                oldRaft {raftDataObservers = M.delete sub (raftDataObservers oldRaft)}
    doUnobserve vRaft endpoint member

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
