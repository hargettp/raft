{-# LANGUAGE ExistentialQuantification #-}

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

import Data.Time

import Network.Endpoints

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

-- class (LogIO l e v) => RaftLog l e v where
--      entryTerm :: e -> Term

data RaftState l e v = (LogIO l e v) => RaftState {
    raftLastUpdate :: UTCTime,
    raftCurrentTerm :: Term,
    raftLastCandidate :: Maybe ServerId,
    raftServer :: Server l e v
}

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
runConsensus :: (LogIO l e v) => Endpoint -> Server l e v -> IO ()
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
      won <- elect raft endpoint
      if won
        then lead raft endpoint
        else return ()
      participate raft

follow :: (LogIO l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
follow vRaft endpoint = do
    f <- async $ doFollow vRaft endpoint
    v <- async $ doVote vRaft endpoint
    w <- async $ doWatch
    _ <- waitAnyCancel [f,v,w]
    return ()
    where
        -- watch for a heartbeat, exiting first if none found
        doWatch = return ()

{-|
    Wait for 'AppendEntries' requests and process them, commit new changes
    as necessary, and stop when no heartbeat received.
-}
doFollow :: (LogIO l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
doFollow vRaft endpoint = do
    success <- onAppendEntries endpoint $ \req -> do
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
                    -- TODO implement correctly
                    (entry:_) -> return (raftCurrentTerm raft1,False)

    if success
        then doFollow vRaft endpoint
        else return ()

{-|
Wait for request vote requests and process them
-}
doVote :: (LogIO l e v) => TVar (RaftState l e v) -> Endpoint -> IO ()
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

elect :: TVar (RaftState l e v) -> Endpoint -> IO Bool
elect _ _ = return False

lead :: TVar (RaftState l e v) -> Endpoint -> IO ()
lead _ _ = return ()
