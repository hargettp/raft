-- | Main entry point to the application.
module Main where

-- local imports

import qualified TestLog as L
import qualified TestRaft as R

-- external imports

import System.Directory
import System.Log.Logger
import System.Log.Handler.Simple

import Test.Framework

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

main :: IO ()
main = do
  initLogging
  defaultMain tests

initLogging :: IO ()
initLogging = do
  let logFile = "tests.log"
  exists <- doesFileExist logFile
  if exists
    then removeFile logFile
    else return ()
  s <- fileHandler logFile INFO
  updateGlobalLogger rootLoggerName (setLevel INFO)
  updateGlobalLogger rootLoggerName (addHandler s)

tests :: [Test.Framework.Test]
tests = [
    ]
    ++ L.tests
    ++ R.tests
