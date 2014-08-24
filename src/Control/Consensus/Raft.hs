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
-- Generalized implementation of the Raft consensus algorithm.
--
-- Application writers generally need only supply an implementation of
-- 'Control.Consensus.Raft.Log.RaftLog' and 'Control.Consensus.Log.State'. For many
-- applications, the provided 'Control.Consensus.Raft.Log.ListLog' may be sufficient for
-- the 'Control.Consensus.Raft.Log.RaftLog' implementation.
-- Applications also may choose to define how to externally persist the application's log and state.
-- Note that use of strong persistence (e.g., to disk or stable storage) or weak persistence
-- (e.g., retain in memory or volatile storage only) is up to the application, with the
-- caveat that Raft can only recover certain failure scenarios when strong persistence
-- is in use.
--
-- The function 'withConsensus' takes the supplied log and state (and an 'Endpoint')
-- and performs the Raft algorithm to ensure consistency with other members of the cluster
-- (all of which must also be executing 'withConsensus' and reachable through their 'Endpoint's).
-- Server applications should execute 'withConsensus', client applications can use the
-- "Control.Consensus.Raft.Client" module to interface with the cluster over a network
-- (or other transport).
--
-----------------------------------------------------------------------------

module Control.Consensus.Raft (
    withConsensus,

    module Data.Log,
    module Control.Consensus.Raft.Actions,
    module Control.Consensus.Raft.Client,
    module Control.Consensus.Raft.Log,
    module Control.Consensus.Raft.Types

) where

-- local imports

import Control.Consensus.Raft.Actions
import Control.Consensus.Raft.Client
import Control.Consensus.Raft.Protocol
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Members
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

import Text.Printf

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

type Requests c = Mailbox (Maybe (ClientRequest c,Reply MemberResult))
type Replies = Mailbox (Index,Reply MemberResult)

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
withConsensus :: (RaftLog l e v) => Endpoint -> Name -> l -> RaftState v -> (Raft l e v -> IO ()) -> IO ()
withConsensus endpoint name initialLog initialState fn = do
    vRaft <- atomically $ mkRaft endpoint initialLog initialState
    withAsync (run vRaft)
        (\_ -> fn vRaft)
    where
        run vRaft = do
            infoM _log $ printf "Starting server %v" name
            finally (catch (participate vRaft)
                        (\e -> case e of
                                ThreadKilled -> return ()
                                _ -> debugM _log $ printf "%v encountered error: %v" name (show (e :: AsyncException))))
                (do
                    infoM _log $ printf "Stopped server %v" name)
        participate vRaft = do
            follow vRaft
            won <- volunteer vRaft
            if won
                then do
                    lead vRaft
                else return ()
            participate vRaft

follow :: (RaftLog l e v) => Raft l e v -> IO ()
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
    As a follower, wait for 'AppendEntries' requests and process them,
    commit new changes as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l e v) => Raft l e v -> IO ()
doFollow vRaft = do
    doSynchronize vRaft onRequest alwaysContinue
    where
        onRequest raft req = do
            let log = raftLog raft
                nextIndex = (1 + (logIndex $ aePreviousTime req))
                count = length $ aeEntries req
            if count > 0
                then infoM _log $ printf "%v: Appending %d at %d" (raftName raft) count nextIndex
                else return ()
            newLog <- appendEntries log nextIndex (aeEntries req)
            if count > 0
                then infoM _log $ printf "%v: Appended %d at %d" (raftName raft) count (lastAppended newLog)
                else return ()
            updatedRaft <- atomically $ do
                    preCommitConfigurationChange vRaft (aeEntries req)
                    modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftLog newLog oldRaft
                    readTVar (raftContext vRaft)
            if (logIndex $ aeCommittedTime req) > (lastCommitted $ raftLog updatedRaft)
                then do
                    infoM _log $ printf "%v: Committing at %d" (raftName raft) (logIndex $ aeCommittedTime req)
                    (committedLog,committedState) <- commitEntries (raftLog updatedRaft) (logIndex $ aeCommittedTime req) (raftState updatedRaft)
                    infoM _log $ printf "%v: Committed at %d" (raftName raft) (lastCommitted committedLog)
                    (checkpointedLog,checkpointedState) <- checkpoint committedLog committedState
                    checkpointedRaft <- atomically $ do
                        modifyTVar (raftContext vRaft) $ \oldRaft ->
                                setRaftLog checkpointedLog
                                $ setRaftState checkpointedState oldRaft
                        readTVar (raftContext vRaft)
                    return checkpointedRaft
                else return updatedRaft
        alwaysContinue _ = True

doSynchronize :: (RaftLog l e v) => Raft l e v -> ((RaftContext l e v) -> AppendEntries e -> IO (RaftContext l e v)) -> (Name -> Bool) -> IO ()
doSynchronize vRaft reqfn contfn = do
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint initialRaft
        cfg = raftStateConfiguration $ raftState initialRaft
        name = raftName initialRaft
    maybeLeader <- onAppendEntries endpoint cfg name $ \req -> do
        raft <- atomically $ do
            raft <-readTVar (raftContext vRaft)
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return raft
                else do
                    modifyTVar (raftContext vRaft) $ \oldRaft ->
                            setRaftTerm (aeLeaderTerm req)
                                $ setRaftLeader (Just $ aeLeader req)
                                $ setRaftLastCandidate Nothing oldRaft
                    readTVar (raftContext vRaft)
        -- check previous entry for consistency
        let requestPreviousTerm = (logTerm $ aePreviousTime req)
            prevIndex = (logIndex $ aePreviousTime req)
        localPreviousTerm <- if prevIndex < 0
            then return (-1)
            else do
                prevEntries <- fetchEntries (raftLog raft) prevIndex 1
                return $ entryTerm $ head prevEntries
        let valid = requestPreviousTerm == localPreviousTerm
        synchronizedRaft <- if valid
                then reqfn raft req
                else return raft
        return $ mkResult valid synchronizedRaft
    case maybeLeader of
        -- we did hear a message before the timeout, and we have a committed time
        Just leader ->  do
            if contfn leader
                then doSynchronize vRaft reqfn contfn
                else return ()
        Nothing -> return ()

preCommitConfigurationChange :: (RaftLog l e v) => Raft l e v -> [RaftLogEntry e] -> STM ()
preCommitConfigurationChange _ [] = return ()
preCommitConfigurationChange vRaft (entry:_) = do
    case entryAction entry of
        Cmd _ -> return ()
        action -> do
            raft <- readTVar (raftContext vRaft)
            let oldRaftState = raftState raft
                newCfg = applyConfigurationAction (clusterConfiguration $ raftStateConfiguration oldRaftState) action
                newRaft = setRaftConfiguration newCfg raft
            writeTVar (raftContext vRaft) newRaft
    -- we don't need to recurse, because we expect the configuration change at the beginning
    -- preCommitConfigurationChange vRaft entries

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l e v) => Raft l e v -> IO ()
doVote vRaft = do
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint initialRaft
        cfg = raftStateConfiguration $ raftState initialRaft
        name = raftName initialRaft
        leading = (Just name) == (clusterLeader $ clusterConfiguration cfg)
    votedForCandidate <- onRequestVote endpoint name $ \req -> do
        debugM _log $ printf "Server %v received vote request from %v" name (show $ rvCandidate req)
        (raft,vote,reason) <- atomically $ do
            raft <- readTVar (raftContext vRaft)
            let log = raftLog raft
            if (rvCandidateTerm req) < (raftCurrentTerm raft)
                then return (raft,False,"Candidate term too old")
                else do
                    modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftTerm (rvCandidateTerm req) oldRaft
                    case raftStateLastCandidate $ raftState raft of
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
volunteer :: (RaftLog l e v) => Raft l e v -> IO Bool
volunteer vRaft = do
    participant <- atomically $ do
        raft <- readTVar $ raftContext vRaft
        let name = raftName raft
            cfg = raftStateConfiguration $ raftState raft
        return $ isClusterParticipant name (clusterConfiguration cfg)
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

doVolunteer :: (RaftLog l e v) => Raft l e v -> IO Bool
doVolunteer vRaft = do
    raft <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft ->
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftLastCandidate Nothing oldRaft
        readTVar (raftContext vRaft)
    let candidate = raftName raft
        cfg = raftStateConfiguration $ raftState raft
        endpoint = raftEndpoint raft
        members = clusterMembers $ clusterConfiguration cfg
        cs = newCallSite endpoint candidate
        term = raftCurrentTerm raft
        log = raftLog raft
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

lead :: (RaftLog l e v) => Raft l e v -> IO ()
lead vRaft = do
    raft <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft -> do
            let name = raftName oldRaft
                cfg = raftStateConfiguration $ raftState oldRaft
                members = mkMembers cfg $ lastCommittedTime $ raftLog oldRaft
            setRaftLeader (Just name)
                $ setRaftLastCandidate Nothing 
                $ setRaftMembers members oldRaft
        readTVar (raftContext vRaft)
    let name = raftName raft
        term = raftCurrentTerm raft
    replies <- atomically $ newMailbox
    requests <- atomically $ newMailbox
    infoM _log $ printf "Server %v leading in term %v with members %s" name (show term) (show $ raftMembers raft)
    raceAll_ $ [doPulse vRaft requests,
                doVote vRaft,
                doRespond vRaft,
                doServe vRaft requests,
                doPerform vRaft requests replies]

doPulse :: (RaftLog l e v) => Raft l e v -> Requests c -> IO ()
doPulse vRaft requests = do
    raft <- atomically $ do
        raft <- readTVar (raftContext vRaft)
        empty <- isEmptyMailbox requests
        if empty
            then writeMailbox requests Nothing
            else return ()
        return raft
    let cfg = raftStateConfiguration $ raftState raft
    threadDelay $ timeoutPulse $ clusterTimeouts cfg
    doPulse vRaft requests

{-|
    As a leader, wait for 'AppendEntries' requests and process them, 
    commit new changes as necessary, and stop when no heartbeat received.
-}
doRespond :: (RaftLog l e v) => Raft l e v -> IO ()
doRespond vRaft = do
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let name = raftName initialRaft
    doSynchronize vRaft onRequest (continueIfLeader name)
    where
        onRequest raft _ = return raft
        continueIfLeader name leader = name == leader

{-|
Service requests from clients
-}
doServe :: (RaftLog l e v) => Raft l e v -> Requests e -> IO ()
doServe vRaft requests = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint raft
        leader = raftName raft
    onPerformAction endpoint leader $ \req reply -> do
        atomically $ writeMailbox requests $ Just (req,reply)
    doServe vRaft requests

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doPerform :: (RaftLog l e v) => Raft l e v -> Requests e -> Replies -> IO ()
doPerform vRaft requests replies = do
    append vRaft requests replies
    commit vRaft replies
    doPerform vRaft requests replies

append :: (RaftLog l e v) => Raft l e v -> Requests e -> Replies -> IO ()
append vRaft actions replies = do
    (raft,maybeAction) <- atomically $ do
        maybeAction <- readMailbox actions
        raft <- readTVar (raftContext vRaft)
        return (raft,maybeAction)
    case maybeAction of
        Nothing -> return ()
        Just (req,reply) -> do
            let action = clientRequestAction req
                oldLog = raftLog raft
                oldState = raftState raft
                term = raftCurrentTerm raft
                nextIndex = (lastAppended oldLog) + 1
                revisedAction = case action of
                    Cmd _ -> action
                    cfgAction -> Cfg $ SetConfiguration $ applyConfigurationAction (clusterConfiguration $ raftStateConfiguration oldState) cfgAction
            newLog <- appendEntries oldLog nextIndex [RaftLogEntry term revisedAction]
            (revisedLog,revisedState) <- case revisedAction of
                Cfg (SetConfiguration newCfg) -> do
                    let newState = oldState {
                        raftStateConfiguration = (raftStateConfiguration oldState) {
                            clusterConfiguration = newCfg
                        },
                        raftStateConfigurationIndex = case newCfg of
                            JointConfiguration _ _ -> Just nextIndex
                            _ -> Nothing
                        }
                    checkpoint newLog newState
                _ -> return (newLog,oldState)
            atomically $ do
                modifyTVar (raftContext vRaft) $ \oldRaft ->
                    setRaftLog revisedLog $ setRaftState revisedState oldRaft
                writeMailbox replies (lastAppended revisedLog,reply)
            infoM _log $ printf "%v: Appended 1 action at index %v" (raftName raft) nextIndex
            return ()

commit :: (RaftLog l e v) => Raft l e v -> Replies -> IO ()
commit vRaft replies = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let initialLog = raftLog raft
        leader = raftName raft
        endpoint = raftEndpoint raft
        cs = newCallSite endpoint leader
        term = raftCurrentTerm raft
        cfg = raftStateConfiguration $ raftState raft
    (prevTime,entries) <- gatherUnsynchronizedEntries (raftMembers raft) (clusterConfiguration cfg) initialLog
    infoM _log $ printf "%v: Broadcasting %d entries after %d" leader (length entries) (logIndex $ prevTime)
    results <- goAppendEntries cs cfg term prevTime (lastCommittedTime initialLog) entries
    let members = raftMembers raft
        newMembers = updateMembers members results
        newCommittedIndex = membersSafeAppendedIndex newMembers $ clusterConfiguration cfg
        newTerm = membersHighestTerm newMembers
    infoM _log $ printf "%v: Member appended indexes are %v" leader (show $ membersAppendedIndex newMembers $ clusterConfiguration cfg)
    atomically $ modifyTVar (raftContext vRaft) $ \oldRaft ->
        setRaftMembers newMembers oldRaft
    if newTerm > (raftCurrentTerm raft)
        then do
            atomically $ modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftTerm newTerm oldRaft
            infoM _log $ printf "Leader stepping down; new term %v discovered" (show newTerm)
            return ()
        else do
            newRaft <- atomically $ readTVar $ raftContext vRaft
            (newLog,newState) <- if (newCommittedIndex > lastCommitted initialLog) 
                then do
                    infoM _log $ printf "%v: Committing at %d" leader newCommittedIndex
                    (log,state) <- commitEntries initialLog newCommittedIndex (raftState newRaft)
                    checkpoint log state
                else return (initialLog,raftState newRaft)
            (revisedLog,revisedState) <- case raftStateConfigurationIndex newState of
                Nothing -> return (newLog,newState)
                Just cfgIndex ->
                    if lastCommitted newLog >= cfgIndex
                        then do
                            let revisedCfg = case (clusterConfiguration $ raftStateConfiguration newState) of
                                    JointConfiguration _ jointNew -> jointNew
                                    newCfg -> newCfg
                            revisedLog <- appendEntries newLog ((lastAppended newLog) + 1)
                                    [RaftLogEntry (raftStateCurrentTerm newState) (Cfg $ SetConfiguration revisedCfg)]
                            let revisedState = newState {raftStateConfigurationIndex = Nothing}
                            return (revisedLog,revisedState)
                        else return (newLog,newState)
            atomically $ modifyTVar (raftContext vRaft) $ \oldRaft ->
                setRaftLog revisedLog
                    $ setRaftState revisedState
                        $ setRaftMembers newMembers oldRaft
            notifyClients vRaft replies revisedLog revisedState
    return ()

gatherUnsynchronizedEntries :: (RaftLog l e v) => Members -> Configuration -> l -> IO (RaftTime,[RaftLogEntry e])
gatherUnsynchronizedEntries members cfg log = do
    let prevIndex = minimum $ membersAppendedIndex members cfg
        startIndex = prevIndex + 1
    if prevIndex < 0
        then do
            let count = (lastAppended log - startIndex) + 1
            entries <- fetchEntries log startIndex count
            return (initialRaftTime,groupEntries entries)
        else do
            let count = (lastAppended log - prevIndex) + 1
            (prev:rest) <- fetchEntries log prevIndex count
            return (RaftTime (entryTerm prev) prevIndex,groupEntries rest)
    where
        -- we return a list of entries that either has 1 configuration
        -- entry at the beginning (e.g., 'Cfg') and no others, or has no
        -- 'Cfg' entry at all.
        groupEntries [] = []
        groupEntries (first:rest) = if isConfigurationEntry first
            then (first:commandEntries rest)
            else (commandEntries (first:rest))
        isConfigurationEntry = isConfigurationAction . entryAction
        isCommandEntry = isCommandAction . entryAction
        commandEntries = takeWhile isCommandEntry

notifyClients :: (RaftLog l e v) => Raft l e v -> Replies -> l -> RaftState v -> IO ()
notifyClients vRaft replies newLog newState = do
    maybeReply <- atomically $ do
        modifyTVar (raftContext vRaft) $ \oldRaft ->
            setRaftLog newLog $ setRaftState newState oldRaft
        maybeClient <- tryPeekMailbox replies
        case maybeClient of
            Nothing -> return Nothing
            Just (index,reply) ->
                if index <= (lastCommitted newLog)
                    then do
                        updatedRaft <- readTVar (raftContext vRaft)
                        _ <- readMailbox replies
                        return $ Just $ reply $ mkResult True updatedRaft
                    else return Nothing
    case maybeReply of
        Nothing -> do
            return ()
        Just reply -> reply

doRedirect :: (RaftLog l e v) => Raft l e v -> IO ()
doRedirect vRaft = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint raft
        member = raftName raft
    onPassAction endpoint member $ \reply -> do
        newRaft <- atomically $ readTVar (raftContext vRaft)
        reply $ mkResult False newRaft
    doRedirect vRaft

mkResult :: (RaftLog l e v) => Bool -> RaftContext l e v -> MemberResult
mkResult success raft = MemberResult {
    memberActionSuccess = success,
    memberLeader = clusterLeader $ clusterConfiguration $ raftStateConfiguration $ raftState raft,
    memberCurrentTerm = raftCurrentTerm raft,
    memberLastAppended = lastAppendedTime $ raftLog raft,
    memberLastCommitted = lastCommittedTime $ raftLog raft
}

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
