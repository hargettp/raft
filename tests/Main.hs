-- | Main entry point to the application.
module Main where

-- local imports

import qualified TestLog as L
import qualified TestRaft as R

-- external imports

import Control.Applicative
import Control.Concurrent

import System.Directory
import System.Environment
import System.Info
import System.IO
import System.Log.Formatter
import System.Log.Handler (setFormatter)
import System.Log.Handler.Simple
import System.Log.Logger

import Test.Framework

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

main :: IO ()
main = do
  initLogging
  printPlatform
  testsToRun <- tests
  raftTransport <- lookupEnv "RAFT_TRANSPORT"
  -- if we're just running the memory transport, let's go for a speedup
  if (raftTransport == Just "mem") || (raftTransport == Nothing)
    then defaultMainWithArgs testsToRun ["-j8"]
    else defaultMain testsToRun

initLogging :: IO ()
initLogging = do
  let logFile = "tests.log"
  exists <- doesFileExist logFile
  if exists
    then removeFile logFile
    else return ()
  s <- streamHandler stdout INFO
  let fs = setFormatter s $ simpleLogFormatter "$time [$prio] - $msg"
  updateGlobalLogger rootLoggerName (setLevel WARNING)
  updateGlobalLogger rootLoggerName $ setHandlers [fs]

printPlatform :: IO ()
printPlatform = do
    putStrLn $ "OS: " ++ os ++ "/" ++ arch
    putStrLn $ "Compiler: " ++ compilerName ++ " " ++ (show compilerVersion)
    capabilities <- getNumCapabilities
    putStrLn $ "Capabilities: " ++ (show capabilities)
    putStrLn ""

tests :: IO [Test.Framework.Test]
tests = (++)
    <$> L.tests
    <*> R.tests
