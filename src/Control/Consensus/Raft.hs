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

import Text.Printf

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
    vRaft <- atomically $ mkRaft endpoint server
    withAsync (run vRaft)
        (\_ -> fn vRaft)
    where
        run vRaft = do
            infoM _log $ printf "Starting server %v" $ serverName server
            finally (catch (participate vRaft)
                        (\e -> case e of
                                ThreadKilled -> return ()
                                _ -> debugM _log $ printf "%v encountered error: %v" (serverName server) (show (e :: AsyncException))))
                (do
                    infoM _log $ printf "Stopped server %v" $ serverName server )
        participate vRaft = do
            follow vRaft
            won <- volunteer vRaft
            if won
                then do
                    lead vRaft
                else return ()
            participate vRaft

follow :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> IO ()
follow vRaft = do
    initialRaft <- atomically $ do
        modifyTVar vRaft $ \oldRaft -> setRaftLeader Nothing oldRaft
        readTVar vRaft
    let name = serverName $ raftServer initialRaft
        term = raftCurrentTerm initialRaft
    infoM _log $ printf "Server %v following in term %v" name term
    raceAll_ [doFollow vRaft False,
                doVote vRaft False,
                doRedirect vRaft]

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> Bool -> IO ()
doFollow vRaft leading = do
    -- TOD what if we're the leader and we're processing a message
    -- we sent earlier?
    initialRaft <- atomically $ readTVar vRaft
    let endpoint = raftEndpoint initialRaft
        server = raftServer initialRaft
        cfg = serverConfiguration $ serverState server
        name = serverName server
    maybeCommitted <- onAppendEntries endpoint cfg name $ \req -> do
        debugM _log $ printf "Server %v received %v" name (show req)
        infoM _log $ printf "Server %v received pulse from %v" name (show $ aeLeader req)
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
                    atomically $ do
                        modifyTVar vRaft $ \oldRaft ->
                                setRaftLog log $ setRaftState state oldRaft
                else return ()
            if leading && (name /= leader)
                then return ()
                else doFollow vRaft leading
        -- we heard no message before timeout
        Nothing -> return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => TVar (RaftContext l v) -> Bool -> IO ()
doVote vRaft leading = do
    initialRaft <- atomically $ readTVar vRaft
    let endpoint = raftEndpoint initialRaft
        name = serverName $ raftServer initialRaft
    won <- onRequestVote endpoint name $ \req -> do
        debugM _log $ printf "Server %v received vote request from %v" name (show $ rvCandidate req)
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
        infoM _log $ printf "Server %v vote for %v is %v because %v" name (rvCandidate req) (show (raftCurrentTerm raft,vote)) reason
        return $ mkResult vote raft
    if leading && won
        then return ()
        else doVote vRaft leading

{-|
Initiate an election, volunteering to lead the cluster if elected.
-}
volunteer :: (RaftLog l v) => TVar (RaftContext l v) -> IO Bool
volunteer vRaft = do
    results <- race (doVote vRaft False) (doVolunteer vRaft)
    case results of
        Left _ -> return False
        Right won -> return won

doVolunteer :: (RaftLog l v) => TVar (RaftContext l v) -> IO Bool
doVolunteer vRaft = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let candidate = serverName $ raftServer raft
        cfg = serverConfiguration $ serverState $ raftServer raft
        endpoint = raftEndpoint raft
        members = clusterMembers cfg
        cs = newCallSite endpoint candidate
        term = raftCurrentTerm raft
        log = serverLog $ raftServer raft
        RaftTime lastTerm lastIndex = lastAppendedTime log
    infoM _log $ printf "Server %v volunteering in term %v" candidate (show $ raftCurrentTerm raft)
    debugM _log $ printf "Server %v is soliciting votes from %v" candidate (show members)
    votes <- goRequestVote cs cfg term candidate (RaftTime lastTerm lastIndex)
    debugM _log $ printf "Server %v (%v) received votes %v" candidate (show (term,lastIndex)) (show votes)
    return $ wonElection votes
    where
        wonElection :: M.Map Name (Maybe MemberResult) -> Bool
        wonElection votes = majority votes $ M.foldl (\tally ballot -> 
            case ballot of
                Just (result) -> if (memberActionSuccess result) then tally + 1 else tally 
                _ -> tally) 0 votes
        majority :: M.Map Name (Maybe MemberResult) -> Int -> Bool
        majority votes tally = tally > ((M.size $ votes) `quot` 2)

lead :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> IO ()
lead vRaft = do
    raft <- atomically $ do
        modifyTVar vRaft $ \oldRaft -> do
            let name = serverName $ raftServer oldRaft
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar vRaft
    let name = serverName $ raftServer raft
        cfg = serverConfiguration $ serverState $ raftServer raft
        members = mkMembers cfg
        term = raftCurrentTerm raft
    clients <- atomically $ newMailbox
    actions <- atomically $ newMailbox
    infoM _log $ printf "Server %v leading in term %v" name (show term)
    raceAll_ $ [doPulse vRaft actions,
                doVote vRaft True,
                doFollow vRaft True,
                doServe vRaft actions,
                doPerform vRaft members clients actions]

type Actions = Mailbox (Maybe (Action,Reply MemberResult))
type Clients = Mailbox (Index,Reply MemberResult)

doPulse :: (RaftLog l v) => TVar (RaftContext l v) -> Actions -> IO ()
doPulse vRaft actions = do
    raft <- atomically $ do
        raft <- readTVar vRaft
        empty <- isEmptyMailbox actions
        if empty
            then writeMailbox actions Nothing
            else return ()
        return raft
    let cfg = serverConfiguration $ serverState $ raftServer raft
    threadDelay $ timeoutPulse $ configurationTimeouts cfg
    doPulse vRaft actions

doServe :: (RaftLog l v) => TVar (RaftContext l v) -> Actions -> IO ()
doServe vRaft actions = do
    raft <- atomically $ readTVar vRaft
    let endpoint = raftEndpoint raft
        leader = serverName $ raftServer raft
    infoM _log $ printf "Serving from %v in term %v "leader (show $ raftCurrentTerm raft)
    onPerformAction endpoint leader $ \action reply -> do
        infoM _log $ printf "Leader %v received action %v" leader (show action)
        atomically $ writeMailbox actions $ Just (action,reply)
    doServe vRaft actions

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doPerform :: (RaftLog l v,Serialize v) => TVar (RaftContext l v) -> M.Map Name Member -> Clients -> Actions -> IO ()
doPerform vRaft members clients actions = do
    append vRaft clients actions
    newMembers <- commit vRaft clients members
    doPerform vRaft newMembers clients actions

append :: (RaftLog l v) => TVar (RaftContext l v) -> Clients -> Actions -> IO ()
append vRaft callers actions = do
    (raft,maybeAction) <- atomically $ do
        maybeAction <- readMailbox actions
        raft <- readTVar vRaft
        return (raft,maybeAction)
    case maybeAction of
        Nothing -> return ()
        Just (action,reply) -> do
            let oldLog = serverLog $ raftServer raft
                term = raftCurrentTerm raft
                nextIndex = (let RaftTime _ i = lastAppendedTime oldLog in i) + 1
            newLog <- appendEntries oldLog nextIndex [RaftLogEntry term action]
            atomically $ do
                modifyTVar vRaft $ \oldRaft -> setRaftLog newLog oldRaft
                writeMailbox callers (lastAppended newLog,reply)
            infoM _log $ printf "Appended action at index %v" (show $ lastAppended newLog)
            return ()

commit :: (RaftLog l v) => TVar (RaftContext l v) -> Clients -> Members -> IO Members
commit vRaft clients members = do
    raft <- atomically $ readTVar vRaft
    let rlog = serverLog $ raftServer raft
        leader = serverName $ raftServer raft
        endpoint = raftEndpoint raft
        cs = newCallSite endpoint leader
        term = raftCurrentTerm raft
        prevTime = lastCommittedTime rlog
        cfg = serverConfiguration $ serverState $ raftServer raft
    (commitIndex,entries) <- fetchLatestEntries rlog
    infoM _log $ printf "Synchronizing from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    results <- goAppendEntries cs cfg term prevTime (RaftTime term commitIndex) entries
    infoM _log $ printf "Synchronized from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    let newMembers = updateMembers members results
        newAppendedIndex = membersSafeAppendedIndex newMembers cfg
        newTerm = membersHighestTerm newMembers
    infoM _log $ printf "Safe appended index is %v" (show newAppendedIndex)
    if newTerm > (raftCurrentTerm raft)
        then do
            infoM _log $ printf "Leader stepping down; new term %v discovered" (show newTerm)
            atomically $ modifyTVar vRaft $ \oldRaft ->
                setRaftTerm newTerm oldRaft
            return ()
        else do
            (newLog,newState) <- let time = (RaftTime (raftCurrentTerm raft) newAppendedIndex)
                                     oldCommitedIndex = commitIndex
                                     count = newAppendedIndex - oldCommitedIndex
                                     in if count > 0
                                        then do
                                            infoM _log $ printf "Committing at time %v" (show time)
                                            commitEntries rlog newAppendedIndex (serverState $ raftServer $ raft)
                                        else return (rlog,(serverState $ raftServer $ raft))
            notifyClients vRaft clients newLog newState
    return newMembers

notifyClients :: (RaftLog l v) => TVar (RaftContext l v) -> Clients -> l -> RaftState v -> IO ()
notifyClients vRaft clients newLog newState = do
    maybeReply <- atomically $ do
        modifyTVar vRaft $ \oldRaft ->
            setRaftLog newLog $ setRaftState newState oldRaft
        maybeClient <- tryPeekMailbox clients
        case maybeClient of
            Nothing -> return Nothing
            Just (index,reply) ->
                if index <= (lastCommitted newLog)
                    then do
                        updatedRaft <- readTVar vRaft
                        _ <- readMailbox clients
                        return $ Just (index, reply $ mkResult True updatedRaft)
                    else return Nothing
    case maybeReply of
        Nothing -> do
            infoM _log $ printf "No client at index %v" (show $ lastCommitted newLog)
            return ()
        Just (index,reply) -> do
            infoM _log $ printf "Replying at index %v" (show index)
            reply
            infoM _log $ printf "Replied at index %v" (show index)

doRedirect :: (RaftLog l v) => TVar (RaftContext l v) -> IO ()
doRedirect vRaft = do
    raft <- atomically $ readTVar vRaft
    let term = raftCurrentTerm raft
        endpoint = raftEndpoint raft
        member = serverName $ raftServer raft
    infoM _log $ printf "Redirecting from %v in %v" member (show term)
    onPerformAction endpoint member $ \action reply -> do
        infoM _log $ printf "Member %v received action %v" member (show action)
        newRaft <- atomically $ readTVar vRaft
        reply $ mkResult False newRaft
    doRedirect vRaft

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
