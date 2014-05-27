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
withConsensus :: (RaftLog l v) => Endpoint -> RaftServer l v -> (Raft l v -> IO ()) -> IO ()
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

follow :: (RaftLog l v) => Raft l v -> IO ()
follow vRaft = do
    initialRaft <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft -> 
            setRaftMembers M.empty 
                $ setRaftLeader Nothing oldRaft
        readTVar (raftContext vRaft)
    let name = raftName initialRaft
        term = raftCurrentTerm initialRaft
    infoM _log $ printf "Server %v following in term %v" name term
    raceAll_ [doFollow vRaft,
                doVote vRaft,
                doRedirect vRaft]

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l v) => Raft l v -> IO ()
doFollow vRaft = do
    -- TOD what if we're the leader and we're processing a message
    -- we sent earlier?
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint initialRaft
        server = raftServer initialRaft
        cfg = raftStateConfiguration $ serverState server
        name = serverName server
        leading = (Just name) == (clusterLeader cfg)
    maybeCommitted <- onAppendEntries endpoint cfg name $ \req -> do
        debugM _log $ printf "Server %v received %v" name (show req)
        infoM _log $ printf "Server %v received pulse from %v" name (show $ aeLeader req)
        (valid, raft) <- atomically $ do
            raft <-readTVar (raftContext vRaft)
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return (False,raft)
                else do
                    -- first update raft state
                    modifyTVar (raftContext vRaft) $ \oldRaft ->
                            setRaftTerm (aeLeaderTerm req)
                                $ setRaftLeader (Just $ aeLeader req)
                                $ setRaftLastCandidate Nothing oldRaft
                    let log = serverLog $ raftServer raft
                    -- check previous entry for consistency
                    return ( (lastAppendedTime log) == (aePreviousTime req),raft)
        if valid && not leading
            then do
                let log = serverLog $ raftServer raft
                    index = (1 + (logIndex $ aePreviousTime req))
                newLog <- appendEntries log index (aeEntries req)
                updatedRaft <- atomically $ do
                        preCommitConfigurationChange vRaft (aeEntries req)
                        modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftLog newLog oldRaft
                        readTVar (raftContext vRaft)
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
                    raft <- atomically $ readTVar (raftContext vRaft)
                    (log,state) <- commitEntries (serverLog $ raftServer $ raft) (logIndex $ committed) (serverState $ raftServer $ raft)
                    atomically $ do
                        modifyTVar (raftContext vRaft) $ \oldRaft ->
                                setRaftLog log $ setRaftState state oldRaft
                else return ()
            if leading && (name /= leader)
                -- someone else is sending append entries, so step down
                then return ()
                else doFollow vRaft
        -- we heard no message before timeout
        Nothing -> return ()

preCommitConfigurationChange :: (RaftLog l v) => Raft l v -> [RaftLogEntry] -> STM ()
preCommitConfigurationChange _ [] = return ()
preCommitConfigurationChange vRaft (entry:entries) = do
    case entryAction entry of
        Cmd _ -> return ()
        action -> do
            raft <- readTVar (raftContext vRaft)
            let oldRaftState = serverState $ raftServer raft
                newCfg = applyConfigurationAction (raftStateConfiguration oldRaftState) action
                newRaft = setRaftConfiguration newCfg raft
            writeTVar (raftContext vRaft) newRaft
    preCommitConfigurationChange vRaft entries

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l v) => Raft l v -> IO ()
doVote vRaft = do
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint initialRaft
        server = raftServer initialRaft
        cfg = raftStateConfiguration $ serverState server
        name = serverName server
        leading = (Just name) == (clusterLeader cfg)
    votedForCandidate <- onRequestVote endpoint name $ \req -> do
        debugM _log $ printf "Server %v received vote request from %v" name (show $ rvCandidate req)
        (raft,vote,reason) <- atomically $ do
            raft <- readTVar (raftContext vRaft)
            let log = serverLog $ raftServer raft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raft,False,"Candidate term too old")
                else do
                    modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftTerm (rvCandidateTerm req) oldRaft
                    case raftLastCandidate raft of
                        Just candidate -> do
                            if (candidate == rvCandidate req)
                                then return (raft,True,"Candidate already seen")
                                else return (raft,False,"Already saw different candidate")
                        Nothing -> do
                            modifyTVar (raftContext vRaft) $ \oldRaft ->
                                    setRaftLastCandidate (Just $ rvCandidate req) oldRaft
                            if name == rvCandidate req
                                then return (raft,True,"Voting for self")
                                else do
                                    if (rvCandidateLastEntryTime req) >= (lastAppendedTime log)
                                        then do
                                            modifyTVar (raftContext vRaft) $ \oldRaft -> 
                                                setRaftLastCandidate (Just $ rvCandidate req) oldRaft
                                            return (raft,True,"Candidate log more up to date")
                                        else return (raft,False,"Candidate log out of date")
        infoM _log $ printf "Server %v vote for %v is %v because %v" name (rvCandidate req) (show (raftCurrentTerm raft,vote)) reason
        return $ mkResult vote raft
    if leading && votedForCandidate
        then return ()
        else doVote vRaft

{-|
Initiate an election, volunteering to lead the cluster if elected.
-}
volunteer :: (RaftLog l v) => Raft l v -> IO Bool
volunteer vRaft = do
    participant <- atomically $ do
        raft <- readTVar $ raftContext vRaft
        let name = raftName raft
            cfg = raftStateConfiguration $ serverState $ raftServer raft 
        return $ isClusterParticipant name cfg
    -- this allows us to have raft servers that are up and running,
    -- but they will just patiently wait until they are a participant
    -- before volunteering
    if participant
        then do
            results <- race (doVote vRaft) (doVolunteer vRaft)
            case results of
                Left _ -> return False
                Right won -> return won
        else return False

doVolunteer :: (RaftLog l v) => Raft l v -> IO Bool
doVolunteer vRaft = do
    raft <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar (raftContext vRaft)
    let candidate = raftName raft
        cfg = raftStateConfiguration $ serverState $ raftServer raft
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

lead :: (RaftLog l v) => Raft l v -> IO ()
lead vRaft = do
    raft <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft -> do
            let name = raftName oldRaft
                cfg = raftStateConfiguration $ serverState $ raftServer oldRaft
                members = mkMembers cfg $ lastCommittedTime $ serverLog $ raftServer oldRaft
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing 
                $ setRaftMembers members oldRaft
        readTVar (raftContext vRaft)
    let name = raftName raft
        term = raftCurrentTerm raft
    clients <- atomically $ newMailbox
    actions <- atomically $ newMailbox
    infoM _log $ printf "Server %v leading in term %v" name (show term)
    raceAll_ $ [doPulse vRaft actions,
                doVote vRaft,
                doFollow vRaft,
                doServe vRaft actions,
                doPerform vRaft actions clients]

type Actions = Mailbox (Maybe (Action,Reply MemberResult))
type Clients = Mailbox (Index,Reply MemberResult)

doPulse :: (RaftLog l v) => Raft l v -> Actions -> IO ()
doPulse vRaft actions = do
    raft <- atomically $ do
        raft <- readTVar (raftContext vRaft)
        empty <- isEmptyMailbox actions
        if empty
            then writeMailbox actions Nothing
            else return ()
        return raft
    let cfg = raftStateConfiguration $ serverState $ raftServer raft
    threadDelay $ timeoutPulse $ clusterTimeouts cfg
    doPulse vRaft actions

doServe :: (RaftLog l v) => Raft l v -> Actions -> IO ()
doServe vRaft actions = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint raft
        leader = raftName raft
    infoM _log $ printf "Serving from %v in term %v "leader (show $ raftCurrentTerm raft)
    onPerformAction endpoint leader $ \action reply -> do
        infoM _log $ printf "Leader %v received action %v" leader (show action)
        atomically $ writeMailbox actions $ Just (action,reply)
    doServe vRaft actions

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doPerform :: (RaftLog l v) => Raft l v -> Actions -> Clients -> IO ()
doPerform vRaft actions clients = do
    append vRaft actions clients
    commit vRaft clients
    doPerform vRaft actions clients

append :: (RaftLog l v) => Raft l v -> Actions -> Clients -> IO ()
append vRaft actions clients = do
    (raft,maybeAction) <- atomically $ do
        maybeAction <- readMailbox actions
        raft <- readTVar (raftContext vRaft)
        return (raft,maybeAction)
    case maybeAction of
        Nothing -> return ()
        Just (action,reply) -> do
            let oldLog = serverLog $ raftServer raft
                term = raftCurrentTerm raft
                nextIndex = (lastAppended oldLog) + 1
            newLog <- appendEntries oldLog nextIndex [RaftLogEntry term action]
            atomically $ do
                modifyTVar (raftContext vRaft) $ \oldRaft ->
                    setRaftLog newLog $ case action of
                        Cmd _ -> oldRaft
                        -- precommitting configuration changes
                        cfgAction -> let newCfg = applyConfigurationAction (raftStateConfiguration $ serverState $ raftServer oldRaft) cfgAction
                                         in setRaftConfiguration newCfg $ setRaftConfigurationIndex (Just nextIndex) oldRaft
                writeMailbox clients (lastAppended newLog,reply)
            infoM _log $ printf "Appended action at index %v" (show $ lastAppended newLog)
            return ()

commit :: (RaftLog l v) => Raft l v -> Clients -> IO ()
commit vRaft clients = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let initialLog = serverLog $ raftServer raft
        leader = raftName raft
        endpoint = raftEndpoint raft
        cs = newCallSite endpoint leader
        term = raftCurrentTerm raft
        prevTime = lastCommittedTime initialLog
        cfg = raftStateConfiguration $ serverState $ raftServer raft
    (commitIndex,entries) <- gatherLatestEntries initialLog
    infoM _log $ printf "Synchronizing from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    results <- goAppendEntries cs cfg term prevTime (RaftTime term commitIndex) entries
    infoM _log $ printf "Synchronized from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    let members = raftMembers raft
        newMembers = updateMembers members results
        newAppendedIndex = membersSafeAppendedIndex newMembers cfg
        newTerm = membersHighestTerm newMembers
    atomically $ modifyTVar (raftContext vRaft) $ \oldRaft ->
        setRaftMembers newMembers oldRaft
    infoM _log $ printf "Safe appended index is %v" (show newAppendedIndex)
    if newTerm > (raftCurrentTerm raft)
        then do
            atomically $ modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftTerm newTerm oldRaft
            infoM _log $ printf "Leader stepping down; new term %v discovered" (show newTerm)
            return ()
        else do
            newRaft <- atomically $ readTVar $ raftContext vRaft
            (newLog,newState) <- let time = (RaftTime (raftCurrentTerm newRaft) newAppendedIndex)
                                     oldCommitedIndex = commitIndex
                                     count = newAppendedIndex - oldCommitedIndex
                                     in if count > 0
                                        then do
                                            infoM _log $ printf "Committing at time %v" (show time)
                                            commitEntries initialLog newAppendedIndex (serverState $ raftServer $ newRaft)
                                        else return (initialLog,(serverState $ raftServer $ newRaft))
            (revisedLog,revisedState) <- case raftStateConfigurationIndex newState of
                Nothing -> return (newLog,newState)
                Just cfgIndex ->
                    if lastCommitted newLog >= cfgIndex
                        then do
                            let newParticipants = case raftStateConfiguration newState of
                                    JointConfiguration _ jointNew -> clusterParticipants jointNew
                                    newCfg -> clusterParticipants newCfg
                            revisedLog <- appendEntries newLog ((lastAppended newLog) + 1)
                                    [RaftLogEntry (raftStateCurrentTerm newState) (SetParticipants newParticipants)]
                            let revisedState = newState {raftStateConfigurationIndex = Nothing}
                            return (revisedLog,revisedState)
                        else return (newLog,newState)
            atomically $ modifyTVar (raftContext vRaft) $ \oldRaft ->
                setRaftLog revisedLog
                    $ setRaftState revisedState
                        $ setRaftMembers newMembers oldRaft
            notifyClients vRaft clients revisedLog revisedState
    return ()


{-|
Return a list of entries, with either a configuration `Action` at the beginning of the list,
or no configuration `Action1 at all in the list. By batching entries in this manner, it becomes
easier to know when to pre-commit a configuration change, and what configuration should be in force
when committing all subsequent entries in the list.
-}
gatherLatestEntries :: (RaftLog l v) => l -> IO (Index,[RaftLogEntry])
gatherLatestEntries log = do
    (commitIndex,entries) <- fetchLatestEntries log
    case entries of
        [] -> return (commitIndex,entries)
        (first:rest) -> if isConfigurationEntry first
                then return (commitIndex,(first:commandEntries rest))
                else return (commitIndex,commandEntries (first:rest))
        where
        isConfigurationEntry = isConfigurationAction . entryAction
        isCommandEntry = isCommandAction . entryAction
        commandEntries = takeWhile isCommandEntry

notifyClients :: (RaftLog l v) => Raft l v -> Clients -> l -> RaftState v -> IO ()
notifyClients vRaft clients newLog newState = do
    maybeReply <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft ->
            setRaftLog newLog $ setRaftState newState oldRaft
        maybeClient <- tryPeekMailbox clients
        case maybeClient of
            Nothing -> return Nothing
            Just (index,reply) ->
                if index <= (lastCommitted newLog)
                    then do
                        updatedRaft <- readTVar (raftContext vRaft)
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

doRedirect :: (RaftLog l v) => Raft l v -> IO ()
doRedirect vRaft = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let term = raftCurrentTerm raft
        endpoint = raftEndpoint raft
        member = raftName raft
    infoM _log $ printf "Redirecting from %v in %v" member (show term)
    onPerformAction endpoint member $ \action reply -> do
        infoM _log $ printf "Member %v received action %v" member (show action)
        newRaft <- atomically $ readTVar (raftContext vRaft)
        reply $ mkResult False newRaft
    doRedirect vRaft

mkResult :: (RaftLog l v) => Bool -> RaftContext l v -> MemberResult
mkResult success raft = MemberResult {
    memberActionSuccess = success,
    memberLeader = clusterLeader $ raftStateConfiguration $ serverState $ raftServer raft,
    memberCurrentTerm = raftCurrentTerm raft,
    memberLastAppended = lastAppendedTime $ serverLog $ raftServer raft,
    memberLastCommitted = lastCommittedTime $ serverLog $ raftServer raft
}

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
