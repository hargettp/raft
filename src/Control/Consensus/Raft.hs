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
                $ setRaftReady False
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
                index = (1 + (logIndex $ aePreviousTime req))
            newLog <- appendEntries log index (aeEntries req)
            updatedRaft <- atomically $ do
                    preCommitConfigurationChange vRaft (aeEntries req)
                    modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftLog newLog oldRaft
                    readTVar (raftContext vRaft)
            (committedLog,committedState) <- commitEntries (raftLog updatedRaft) (logIndex $ aeCommittedTime req) (raftState updatedRaft)
            (checkpointedLog,checkpointedState) <- checkpoint committedLog committedState
            checkpointedRaft <- atomically $ do
                modifyTVar (raftContext vRaft) $ \oldRaft ->
                        setRaftLog checkpointedLog $ setRaftState checkpointedState oldRaft
                readTVar (raftContext vRaft)
            return checkpointedRaft
        alwaysContinue _ = True

doSynchronize :: (RaftLog l e v) => Raft l e v -> ((RaftContext l e v) -> AppendEntries e -> IO (RaftContext l e v)) -> (Name -> Bool) -> IO ()
doSynchronize vRaft reqfn contfn = do
    initialRaft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint initialRaft
        cfg = raftStateConfiguration $ raftState initialRaft
        name = raftName initialRaft
    maybeLeader <- onAppendEntries endpoint cfg name $ \req -> do
        -- debugM _log $ printf "Server %v received %v" name (show req)
        infoM _log $ printf "Server %v received pulse from %v" name (show $ aeLeader req)
        (valid, raft) <- atomically $ do
            raft <-readTVar (raftContext vRaft)
            if (aeLeaderTerm req) < (raftCurrentTerm raft)
                then return (False,raft)
                else do
                    modifyTVar (raftContext vRaft) $ \oldRaft ->
                            setRaftTerm (aeLeaderTerm req)
                                $ setRaftLeader (Just $ aeLeader req)
                                $ setRaftLastCandidate Nothing oldRaft
                    let log = raftLog raft
                    -- check previous entry for consistency
                    return ( (lastAppendedTime log) == (aePreviousTime req),raft)
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
                $ setRaftReady False
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
            setRaftTerm ((raftCurrentTerm oldRaft) + 1)
                $ setRaftReady False
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
                doRespond vRaft,
                doServe vRaft actions,
                doPerform vRaft actions clients]

type Actions c = Mailbox (Maybe (RaftAction c,Reply MemberResult))
type Clients = Mailbox (Index,Reply MemberResult)

doPulse :: (RaftLog l e v) => Raft l e v -> Actions c -> IO ()
doPulse vRaft actions = do
    raft <- atomically $ do
        raft <- readTVar (raftContext vRaft)
        empty <- isEmptyMailbox actions
        if empty
            then writeMailbox actions Nothing
            else return ()
        return raft
    let cfg = raftStateConfiguration $ raftState raft
    threadDelay $ timeoutPulse $ clusterTimeouts cfg
    doPulse vRaft actions

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
doServe :: (RaftLog l e v) => Raft l e v -> Actions e -> IO ()
doServe vRaft actions = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let endpoint = raftEndpoint raft
        leader = raftName raft
    infoM _log $ printf "Serving from %v in term %v "leader (show $ raftCurrentTerm raft)
    onPerformAction endpoint leader $ \action reply -> do
        -- infoM _log $ printf "Leader %v received action %v" leader (show action)
        maybeReply <- atomically $ do
            newRaft <- readTVar (raftContext vRaft)
            -- This is important, as it helps clients see a consistent
            -- view of the cluster: we don't report ourselves as leader
            -- until we are ready, and we're not ready until we have
            -- received a response from a majority of members for
            -- at least 1 `AppendEntries` RPC
            if (raftStateReady $ raftState newRaft)
                then do
                    writeMailbox actions $ Just (action,reply)
                    return Nothing
                else return $ Just $ reply $ mkResult False newRaft
        case maybeReply of
            Nothing -> return ()
            Just failure -> failure
    doServe vRaft actions

{-|
Leaders commit entries to their log, once enough members have appended those entries.
Once committed, the leader replies to the client who requested the action.
-}
doPerform :: (RaftLog l e v) => Raft l e v -> Actions e -> Clients -> IO ()
doPerform vRaft actions clients = do
    append vRaft actions clients
    commit vRaft clients
    doPerform vRaft actions clients

append :: (RaftLog l e v) => Raft l e v -> Actions e -> Clients -> IO ()
append vRaft actions clients = do
    (raft,maybeAction) <- atomically $ do
        maybeAction <- readMailbox actions
        raft <- readTVar (raftContext vRaft)
        return (raft,maybeAction)
    case maybeAction of
        Nothing -> return ()
        Just (action,reply) -> do
            let oldLog = raftLog raft
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
                writeMailbox clients (lastAppended newLog,reply)
            infoM _log $ printf "Appended action at index %v" (show $ lastAppended newLog)
            return ()

commit :: (RaftLog l e v) => Raft l e v -> Clients -> IO ()
commit vRaft clients = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let initialLog = raftLog raft
        leader = raftName raft
        endpoint = raftEndpoint raft
        cs = newCallSite endpoint leader
        term = raftCurrentTerm raft
        prevTime = lastCommittedTime initialLog
        cfg = raftStateConfiguration $ raftState raft
    (commitIndex,entries) <- gatherLatestEntries initialLog
    -- infoM _log $ printf "Synchronizing from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    results <- goAppendEntries cs cfg term prevTime (RaftTime term commitIndex) entries
    -- infoM _log $ printf "Synchronized from %v in term %v: %v" leader (show $ raftCurrentTerm raft) (show entries)
    let members = raftMembers raft
        newMembers = updateMembers members results
        newAppendedIndex = membersSafeAppendedIndex newMembers $ clusterConfiguration cfg
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
            -- we report ourselves ready, because if we are here, then at least a majority
            -- of members have joined our term and thus acknowledged us as leader. Only
            -- once that has happened is it safe to serve clients
            newRaft <- atomically $ do
                -- stronger guarantee than just in our term: members must have agreed
                -- to the action, also implying they agreed with our leadership
                if (not $ isRaftReady raft) && majorityConsent results
                    then modifyTVar (raftContext vRaft) $ \oldRaft -> setRaftReady True oldRaft
                    else return ()
                readTVar $ raftContext vRaft
            (newLog,newState) <- let time = (RaftTime (raftCurrentTerm newRaft) newAppendedIndex)
                                     oldCommitedIndex = commitIndex
                                     count = newAppendedIndex - oldCommitedIndex
                                     in if count > 0
                                        then do
                                            infoM _log $ printf "Committing at time %v" (show time)
                                            (log,state) <- commitEntries initialLog newAppendedIndex (raftState newRaft)
                                            checkpoint log state
                                        else return (initialLog,(raftState newRaft))
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
            notifyClients vRaft clients revisedLog revisedState
    return ()


{-|
Return a list of entries, with either a configuration `Action` at the beginning of the list,
or no configuration `Action1 at all in the list. By batching entries in this manner, it becomes
easier to know when to pre-commit a configuration change, and what configuration should be in force
when committing all subsequent entries in the list.
-}
gatherLatestEntries :: (RaftLog l e v) => l -> IO (Index,[RaftLogEntry e])
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

notifyClients :: (RaftLog l e v) => Raft l e v -> Clients -> l -> RaftState v -> IO ()
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

doRedirect :: (RaftLog l e v) => Raft l e v -> IO ()
doRedirect vRaft = do
    raft <- atomically $ readTVar (raftContext vRaft)
    let term = raftCurrentTerm raft
        endpoint = raftEndpoint raft
        member = raftName raft
    infoM _log $ printf "Redirecting from %v in %v" member (show term)
    onPassAction endpoint member $ \reply -> do
        newRaft <- atomically $ readTVar (raftContext vRaft)
        reply $ mkResult False newRaft
    doRedirect vRaft

mkResult :: (RaftLog l e v) => Bool -> RaftContext l e v -> MemberResult
mkResult success raft = MemberResult {
    memberActionSuccess = success,
    -- If we're the leader, we can't report ourselves as a leader
    -- in results until we are actually ready
    memberLeader = if (isRaftLeader raft) && (not $ isRaftReady raft)
        then Nothing
        else clusterLeader $ clusterConfiguration $ raftStateConfiguration $ raftState raft,
    memberCurrentTerm = raftCurrentTerm raft,
    memberLastAppended = lastAppendedTime $ raftLog raft,
    memberLastCommitted = lastCommittedTime $ raftLog raft
}

raceAll_ :: [IO ()] -> IO ()
raceAll_ actions = do
    tasks <- mapM async actions
    _ <- waitAnyCancel tasks
    return ()
