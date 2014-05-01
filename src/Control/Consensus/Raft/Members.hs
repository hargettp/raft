{-# LANGUAGE DeriveGeneric #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Consensus.Raft.Members
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

module Control.Consensus.Raft.Members (
    Member(..),
    mkMember,
    updateMember,

    Members,
    mkMembers,
    updateMembers,
    membersSafeAppendedIndex,
    membersAppendedIndex,
    membersSafeCommittedIndex,
    membersCommittedIndex,
    membersHighestTerm,

    MemberResult(..),
    mkResult,

    MemberResults
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
import Control.Consensus.Raft.Log
import Control.Consensus.Raft.Types

-- external imports

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import Data.Serialize

import GHC.Generics

import Network.Endpoints

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

data Member = Member {
    memberName :: Name,
    memberLogLastAppended :: RaftTime,
    memberLogLastCommitted :: RaftTime
} deriving (Show)

memberAppendedTerm :: Member -> Term
memberAppendedTerm member = let RaftTime term _ = memberLogLastAppended member
                        in term

mkMember :: Name -> Member
mkMember name = Member {
    memberName = name,
    memberLogLastAppended = RaftTime (-1) (-1),
    memberLogLastCommitted = RaftTime (-1) (-1)
    }

updateMember :: Member -> MemberResult -> Member
updateMember member result = member {
    memberLogLastAppended = max (memberLogLastAppended member) (memberLastAppended result),
    memberLogLastCommitted = max (memberLogLastCommitted member) (memberLastCommitted result)
    }

type Members = M.Map Name Member

mkMembers :: Configuration -> Members
mkMembers cfg = M.fromList $ map (\name -> (name,mkMember name)) (clusterMembers cfg)

data MemberResult = MemberResult {
    memberActionSuccess :: Bool,
    memberLeader :: Maybe Name,
    memberCurrentTerm :: Term,
    memberLastAppended :: RaftTime,
    memberLastCommitted :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize MemberResult

mkResult :: (RaftLog l v) => Bool -> RaftContext l v -> MemberResult
mkResult success raft = MemberResult {
    memberActionSuccess = success,
    memberLeader = clusterLeader $ serverConfiguration $ serverState $ raftServer raft,
    memberCurrentTerm = raftCurrentTerm raft,
    memberLastAppended = lastAppendedTime $ serverLog $ raftServer raft,
    memberLastCommitted = lastCommittedTime $ serverLog $ raftServer raft
}

type MemberResults =  M.Map Name (Maybe MemberResult)

updateMembers :: Members -> MemberResults -> Members
updateMembers members results = M.map applyUpdates members
    where
        applyUpdates member =
            case M.lookup (memberName member) results of
                Nothing -> member
                Just Nothing -> member
                Just (Just result) -> updateMember member result

{-|
Find the highest log entry `Index` that has already been appended
on a majority of members. We do so by sorting members based on their
last appended log index (irrespective of term), then picking the
value that is less than or equal to the highest appended
log entry index on the majority of servers.
-}
membersSafeAppendedIndex :: Members -> Configuration -> Index
membersSafeAppendedIndex members cfg =
    (membersAppendedIndex members cfg) !! (majority cfg)
    where
        majority (Configuration _ participants _ _) = (S.size participants) `quot` 2
        majority (JointConfiguration jointOld jointNew) = min (majority jointOld) (majority jointNew)

membersAppendedIndex :: Members -> Configuration -> [Index]
membersAppendedIndex members cfg = 
    map (logIndex . memberLogLastAppended) sortedParticipants
    where
        sortedParticipants = L.sortBy byAppendedIndex $ 
            filter (\mbr -> elem (memberName mbr) $ clusterParticipants cfg) $ M.elems members
        byAppendedIndex left right =
            let leftIndex = logIndex $ memberLogLastAppended left
                rightIndex = logIndex $ memberLogLastAppended right
            -- inverting the ordering so that we sort from high to low
            in compare rightIndex leftIndex

{-|
Find the highest log entry `Index` that has already been committed
on a majority of members. We do so by sorting members based on their
last committed log index (irrespective of term), then picking the
value that is less than or equal to the highest committed
log entry index on the majority of servers.
-}
membersSafeCommittedIndex :: Members -> Index
membersSafeCommittedIndex members =
    (membersCommittedIndex members) !! majority
    where
        majority = (M.size members) `quot` 2

membersCommittedIndex :: Members -> [Index]
membersCommittedIndex members = 
    map (logIndex . memberLogLastCommitted) sortedMembers
    where
        sortedMembers = L.sortBy byCommittedIndex $ M.elems members
        byCommittedIndex left right = 
            let leftIndex = logIndex $ memberLogLastCommitted left
                rightIndex = logIndex $ memberLogLastCommitted right
            -- inverting the ordering so that we sort from high to low
            in compare rightIndex leftIndex

membersHighestTerm :: Members -> Term
membersHighestTerm members = maximum $ map memberAppendedTerm $ M.elems members