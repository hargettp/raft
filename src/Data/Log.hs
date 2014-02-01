-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Log
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

module Data.Log (

    Log(..),
    LogEntry(..) --,
    -- commit

) where

-- local imports

import Control.Consensus.Raft.Types

-- external imports

import Prelude hiding (log)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

type Action a = a -> a

data LogEntry a = LogEntry {
    entryAction :: Action a
}

data Log a = Log {
    logLastCommittedIndex :: IO Index,
    logLastAppendedIndex :: IO Index,
    logAppendEntries :: Index -> [LogEntry a] -> IO (),
    logFetchEntries :: Index -> Int -> IO [LogEntry a],
    logCommit :: a -> Index -> IO a
}
