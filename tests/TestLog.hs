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
-- (..... module description .....)
--
-----------------------------------------------------------------------------

module TestLog (
    tests
) where

-- local imports

import NumberServer

-- external imports

import Data.Log

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
    _ <- newNumberLog
    return ()

testEmptyLog :: Assertion
testEmptyLog = do
    log <- newNumberLog
    let val = 0
    chg <- (logCommit log) 0 val
    assertEqual "Empty log should leave value unchanged" val chg

testSingleAction :: Assertion
testSingleAction = do
    log <- newNumberLog
    (logAppendEntries log) 0 [LogEntry (+ 2)]
    let val = 1
    entries <- (logFetchEntries) log 0 1
    lastIndex <- (logLastAppendedIndex log)
    assertEqual "Log index should be 0" 0 lastIndex
    assertBool "Log should not be empty" (not $ null entries)
    chg <- (logCommit log) 0 val
    assertEqual "Committing simple log did not match expected value" 3 chg
    committedIndex <- (logLastCommittedIndex log)
    assertEqual "Committed index sould be equal to lastIndex" lastIndex committedIndex

testDoubleAction :: Assertion
testDoubleAction = do
    log <- newNumberLog
    (logAppendEntries log) 0 [LogEntry (+ 2),LogEntry ( * 5)]
    let val = 1
    entries <- (logFetchEntries) log 0 1
    lastIndex <- (logLastAppendedIndex log)
    assertEqual "Log index incorrect" 1 lastIndex
    assertBool "Log should not be empty" (not $ null entries)
    chg <- (logCommit log) 1 val
    assertEqual "Committing simple log did not match expected value" 15 chg
    committedIndex <- (logLastCommittedIndex log)
    assertEqual "Committed index sould be equal to lastIndex" lastIndex committedIndex
