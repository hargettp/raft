-----------------------------------------------------------------------------
-- |
-- Module      :  NumberServer
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

module NumberServer (
    newNumberLog
) where

-- local imports

import Data.Log

-- external imports

import Control.Concurrent.STM

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

newNumberLog :: IO (Log Int)
newNumberLog = do
    committedIndex <- atomically $ newTVar $ -1
    lastIndex <- atomically $ newTVar $ -1
    entries <- atomically $ newTVar []
    return Log {
        logLastCommittedIndex = lastCommitted committedIndex,
        logLastIndex = lastAppended lastIndex,
        logAppendEntries = (\index newEntries -> atomically $ do
                                modifyTVar entries $ \oldEntries -> 
                                    (take index oldEntries) ++ newEntries
                                modifyTVar lastIndex $ \oldIndex -> oldIndex + (length newEntries)),
        logFetchEntries = (\index count -> atomically $ fetchEntries entries index count),
        logCommit = (\index state -> atomically $ do
            committed <- readTVar committedIndex
            if index > committed
                then do
                    let nextCommitted = committed + 1
                    uncommitted <- fetchEntries entries nextCommitted (index - committed)
                    commitLog committedIndex nextCommitted uncommitted state
                else return state
            )
    }
    where
        fetchEntries entries index count = do
            existing <- readTVar entries
            return $ take count $ drop index existing
        lastCommitted committedIndex = atomically $ readTVar committedIndex
        lastAppended lastIndex = atomically $ readTVar lastIndex
        commitLog committedIndex index [] state = do
            writeTVar committedIndex $ index - 1
            return state
        commitLog committedIndex index (entry:rest) state = do
            let newState = (entryAction entry) state
            commitLog committedIndex (index + 1) rest newState
