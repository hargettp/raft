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
    reconfigureMembers,
    membersSafeAppendedTerm,
    membersAppendedTerm,
    membersSafeAppendedIndex,
    membersAppendedIndex,
    membersSafeCommittedIndex,
    membersCommittedIndex,
    membersHighestTerm,

    MemberResult(..),

    MemberResults
) where

-- local imports

import Control.Consensus.Log
import Control.Consensus.Raft.Configuration
-- import Control.Consensus.Raft.Log
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
    memberTerm :: Term,
    memberLogLastAppended :: RaftTime,
    memberLogLastCommitted :: RaftTime
} deriving (Eq,Show)

mkMember :: RaftTime -> Name -> Member
mkMember time name = Member {
    memberName = name,
    memberTerm = logTerm time,
    memberLogLastAppended = time,
    memberLogLastCommitted = time
    }

updateMember :: Member -> MemberResult -> Member
updateMember member result = member {
    memberTerm = memberCurrentTerm result,
    memberLogLastAppended = memberLastAppended result,
    memberLogLastCommitted = memberLastCommitted result
    }

type Members = M.Map Name Member

mkMembers :: Configuration -> RaftTime -> Members
mkMembers cfg time = M.fromList $ map (\name -> (name,mkMember time name)) (clusterMembers cfg)

reconfigureMembers :: Members -> Configuration -> RaftTime -> Members
reconfigureMembers members cfg time = 
    M.fromList $ map (\name -> case M.lookup name members of
            Nothing -> (name,mkMember time name)
            Just member -> (name,member)) (clusterMembers cfg)

data MemberResult = MemberResult {
    memberActionSuccess :: Bool,
    memberLeader :: Maybe Name,
    memberCurrentTerm :: Term,
    memberLastAppended :: RaftTime,
    memberLastCommitted :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize MemberResult

type MemberResults =  M.Map Name (Maybe MemberResult)

updateMembers :: Members -> MemberResults -> Members
updateMembers members results = M.map applyUpdates members
    where
        applyUpdates member =
            case M.lookup (memberName member) results of
                Nothing -> member
                Just Nothing -> member
                Just (Just result) -> updateMember member result

membersSafeAppendedTerm :: Members -> Configuration -> Term
membersSafeAppendedTerm members cfg@(Configuration _ _ _ _) =
    if M.null members
        then -1
        else (membersAppendedTerm members cfg) !! majority
    where
        majority = (S.size $ configurationParticipants cfg) `quot` 2
membersSafeAppendedTerm members (JointConfiguration jointOld jointNew) =
    min (membersSafeAppendedTerm members jointOld) (membersSafeAppendedTerm members jointNew)

membersAppendedTerm :: Members -> Configuration -> [Term]
membersAppendedTerm members cfg = 
    map (logTerm . memberLogLastAppended) sortedParticipants
    where
        sortedParticipants = L.sortBy byAppendedTerm $
            filter (\mbr -> elem (memberName mbr) $ clusterParticipants cfg) $ M.elems members
        byAppendedTerm left right =
            let leftTerm = logTerm $ memberLogLastAppended left
                rightTerm = logTerm $ memberLogLastAppended right
            -- inverting the ordering so that we sort from high to low
            in compare rightTerm leftTerm

{-|
Find the highest log entry `Index` that has already been appended
on a majority of members. We do so by sorting members based on their
last appended log index (irrespective of term), then picking the
value that is less than or equal to the highest appended
log entry index on the majority of servers.
-}
membersSafeAppendedIndex :: Members -> Configuration -> Index
membersSafeAppendedIndex members cfg@(Configuration _ _ _ _) =
    if M.null members
        then -1
        else (membersAppendedIndex members cfg) !! majority
    where
        majority = (S.size $ configurationParticipants cfg) `quot` 2
membersSafeAppendedIndex members (JointConfiguration jointOld jointNew) =
    min (membersSafeAppendedIndex members jointOld) (membersSafeAppendedIndex members jointNew)

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
membersHighestTerm members = maximum $ map memberTerm $ M.elems members