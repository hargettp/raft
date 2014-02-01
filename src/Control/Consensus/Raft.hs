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

import Control.Consensus.Raft.Server

-- external imports

import Control.Exception

import Network.Endpoints

import System.Log.Logger

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

_log :: String
_log = "raft.consensus"

{-|
Run the core Raft consensus algorithm for the indicated server.  This function
takes care of coordinating the transitions among followers, candidates, and leaders as necessary.
-}
runConsensus :: Endpoint -> Server a -> IO ()
runConsensus endpoint server = do
  catch run (\e -> errorM _log $ (show $ serverId server)
                  ++ " encountered error: " ++ (show (e :: SomeException)))
  where
    run = do
      infoM _log $ "Starting server " ++ (serverId server)
      finally (do
                  participate)
        (do
            infoM _log $ "Stopped server " ++ (serverId server) )
    participate = do
      follow server endpoint
      won <- elect server endpoint
      if won
        then lead server endpoint
        else return ()
      participate

follow :: Server a -> Endpoint -> IO ()
follow _ _ = return ()

elect :: Server a -> Endpoint -> IO Bool
elect _ _ = return False

lead :: Server a -> Endpoint -> IO ()
lead _ _ = return ()
