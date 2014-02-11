-----------------------------------------------------------------------------
-- |
-- Module      :  TestLog
-- Copyright   :  (c) Phil Hargett 2014
-- License     :  MIT (see LICENSE file)
-- 
-- Maintainer  :  phil@haphazardhouse.net
-- Stability   :  experimental
-- Portability :  non-portable (requires STM)
--
-- Unit tests for basic 'Log' typeclass.
--
-----------------------------------------------------------------------------

module TestLog (
    tests
) where

-- local imports

import Control.Consensus.Raft.Types

import IntServer

-- external imports

import Data.Log
import Data.Serialize

import Prelude hiding (log)

import Test.Framework
import Test.HUnit
import Test.Framework.Providers.HUnit

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

tests :: [Test.Framework.Test]
tests = [
    testCase "new-log" testNewLog,
    testCase "empty-log" testEmptyLog,
    testCase "single-action" testSingleAction,
    testCase "double-action" testDoubleAction
    ]

testNewLog :: Assertion
testNewLog = do
    _ <- newLog :: IO IntLog
    return ()

testEmptyLog :: Assertion
testEmptyLog = do
    log <- newLog :: IO IntLog
    let val = 0 :: Int
    (_,chg) <- commitEntries log 0 val
    assertEqual "Empty log should leave value unchanged" val chg

testSingleAction :: Assertion
testSingleAction = do
    log <- newLog :: IO IntLog
    -- log1 <- appendEntries log 0 [IntLogEntry (Add 2)]
    log1 <- appendEntries log 0 [RaftLogEntry 1 $ Cmd $ encode (Add 2)]
    let val = 1 :: Int
    entries <- fetchEntries log1 0 1
    let lastIndex = lastAppended log1
    assertEqual "Log index should be 0" 0 lastIndex
    assertBool "Log should not be empty" (not $ null entries)
    (log2,chg) <- commitEntries log1 0 val
    assertEqual "Committing simple log did not match expected value" 3 chg
    let committedIndex = lastCommitted log2
    assertEqual "Committed index sould be equal to lastIndex" lastIndex committedIndex

testDoubleAction :: Assertion
testDoubleAction = do
    log <- newLog :: IO IntLog
    -- log1 <- appendEntries log 0 [IntLogEntry (Add 2),IntLogEntry (Multiply 5)]
    log1 <- appendEntries log 0 [RaftLogEntry 1 $ Cmd $ encode (Add 2),RaftLogEntry 1 $ Cmd $ encode (Multiply 5)]
    let val = 1
    entries <- fetchEntries log1 0 2
    assertBool "Log should not be empty" (not $ null entries)
    assertEqual "Length incorrect" 2 (length entries)
    let lastIndex = lastAppended log1
    assertEqual "Appended index incorrect" 1 lastIndex
    (log2,chg) <- commitEntries log1 1 val
    assertEqual "Committing simple log did not match expected value" 15 chg
    let committedIndex = lastCommitted log2
    assertEqual "Committed index sould be equal to lastIndex" lastIndex committedIndex
