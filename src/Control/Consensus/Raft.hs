{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE MultiParamTypeClasses #-}

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
import Control.Consensus.Raft.Server
import Control.Consensus.Raft.Types

import Data.Log

-- external imports

import Control.Concurrent.Async
import Control.Exception
import Control.Concurrent.STM

import qualified Data.Map as M
import Data.Time

import Network.Endpoints
import Network.RPC

import Prelude hiding (log)

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

class RaftLogEntry e where
    entryTerm :: e -> Term

class (RaftLogEntry e,LogIO l e v) => RaftLog l e v

data RaftState l e v = (RaftLog l e v) => RaftState {
    raftLastUpdate :: UTCTime,
    raftCurrentTerm :: Term,
    raftLastCandidate :: Maybe ServerId,
    raftServer :: Server l e v
}

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
runConsensus :: (RaftLog l e v) => Endpoint -> Server l e v -> IO ()
runConsensus endpoint server = do
  catch run (\e -> errorM _log $ (show $ serverId server)
                  ++ " encountered error: " ++ (show (e :: SomeException)))
  where
    run = do
        now <- getCurrentTime
        raft <- atomically $ newTVar $ RaftState {
            raftLastUpdate = now,
            raftCurrentTerm = 0,
            raftLastCandidate = Nothing,
            raftServer = server
        }
        infoM _log $ "Starting server " ++ (serverId server)
        finally (do participate raft)
            (do
                infoM _log $ "Stopped server " ++ (serverId server) )
    participate raft= do
      follow raft endpoint
      won <- volunteer raft endpoint
      if won
        then lead raft endpoint
        else return ()
      participate raft

follow :: (RaftLog l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
follow vRaft endpoint = race_ (doFollow vRaft endpoint) (doVote vRaft endpoint)

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (RaftLog l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
doFollow vRaft endpoint = do
    continue <- onAppendEntries endpoint $ \req -> do
        raft <- atomically $ readTVar vRaft
        if (aeLeaderTerm req) < (raftCurrentTerm raft)
            then return (raftCurrentTerm raft,False)
            else do
                -- first update term
                if (aeLeaderTerm req) > (raftCurrentTerm raft)
                    then atomically $ modifyTVar vRaft $ \oldRaft -> oldRaft {raftCurrentTerm = aeLeaderTerm req}
                    else return ()
                -- now check that we're in sync
                raft1 <- atomically $ readTVar vRaft
                entries <- fetchEntries (serverLog $ raftServer raft) (aePreviousIndex req) 1
                case entries of
                    [] -> return (raftCurrentTerm raft1,False)
                    (entry:_) -> let term = raftCurrentTerm raft1
                                     in return (term,(term == (entryTerm entry)) )

    if continue
        then doFollow vRaft endpoint
        else return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (RaftLog l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
doVote vRaft endpoint = do
    onRequestVote endpoint $ \req -> atomically $ do
        raft <- readTVar vRaft
        if (rvCandidateTerm req) < (raftCurrentTerm raft)
            then return (raftCurrentTerm raft,False)
            else do
                if (rvCandidateTerm req) > (raftCurrentTerm raft)
                    then modifyTVar vRaft $ \oldRaft -> oldRaft {raftCurrentTerm = rvCandidateTerm req}
                    else return ()
                raft1 <- readTVar vRaft
                case raftLastCandidate raft1 of
                    Just candidate -> do
                        if Just candidate == raftLastCandidate raft1
                            then return (raftCurrentTerm raft1,True)
                            else return (raftCurrentTerm raft1,False)
                    Nothing -> do
                        if logOutOfDate raft1 req
                            then do
                                modifyTVar vRaft $ \oldRaft -> oldRaft {raftLastCandidate = Just $ rvCandidate req}
                                return (raftCurrentTerm raft1,True)
                            else return (raftCurrentTerm raft,False)
    doVote vRaft endpoint
    where
        -- check that candidate log is more up to date than this server's log
        logOutOfDate raft req = if (rvCandidateLastEntryTerm req) > (raftCurrentTerm raft)
                                then True
                                else if (rvCandidateLastEntryTerm req) < (raftCurrentTerm raft)
                                    then False
                                    else (lastAppended $ serverLog $ raftServer raft) < (rvCandidateLastEntryIndex req)

volunteer :: (RaftLog l e v) => TVar (RaftState l e v) -> Endpoint -> IO Bool
volunteer vRaft endpoint = do
    raft <- atomically $ readTVar vRaft
    let members = clusterMembers $ serverConfiguration $ raftServer raft
        candidate = serverId $ raftServer raft
        cs = newCallSite endpoint candidate
        term = raftCurrentTerm raft
        log = serverLog $ raftServer raft
        lastIndex = lastAppended log
    lastEntries <- fetchEntries log lastIndex 1
    case lastEntries of
        (entry:[]) -> do
            let lastTerm = entryTerm entry
            votes <- goRequestVote cs members term candidate lastIndex lastTerm
            return $ wonElection votes
        _ -> return False
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

lead :: (RaftLog l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
lead _ _ = return ()
