-- | Main entry point to the application.
module Main where

-- local imports

import qualified TestLog as L
import qualified TestRaft as R

-- external imports

import System.Directory
import System.Info
import System.Log.Logger
import System.Log.Handler.Simple

import Test.Framework

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

main :: IO ()
main = do
  initLogging
  printPlatform
  defaultMain tests

initLogging :: IO ()
initLogging = do
  let logFile = "tests.log"
  exists <- doesFileExist logFile
  if exists
    then removeFile logFile
    else return ()
  s <- fileHandler logFile INFO
  updateGlobalLogger rootLoggerName (setLevel WARNING)
  updateGlobalLogger rootLoggerName (addHandler s)

printPlatform :: IO ()
printPlatform = do
    putStrLn $ "OS: " ++ os ++ "/" ++ arch
    putStrLn $ "Compiler: " ++ compilerName ++ " " ++ (show compilerVersion)
    putStrLn ""

tests :: [Test.Framework.Test]
tests = [
    ]
    ++ L.tests
    ++ R.tests
