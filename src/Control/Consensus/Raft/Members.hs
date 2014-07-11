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
-- Contains utility definitions for maintaining awareness of the state of the members
-- in a clsuter; used only be the active leader of a cluster.
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

    MemberResults,
    majorityConsent
) where

-- local imports

import Data.Log
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

{-|
Describes the state of a member as far as helps with execution of the Raft algorithm.
-}
data Member = Member {
    -- | the member's name
    memberName :: Name,
    -- | the member's current term
    memberTerm :: Term,
    -- | the 'RaftTime' of the last entry appended to the member's 'Control.Consensus.Raft.Log.RaftLog'.
    memberLogLastAppended :: RaftTime,
    -- | the 'RaftTime' of the last entry committed in the member's 'Control.Consensus.Raft.Log.RaftLog'.
    memberLogLastCommitted :: RaftTime
} deriving (Eq,Show)

{-|
Create a new 'Member' for tracking the state of a cluster member.
-}
mkMember :: RaftTime -> Name -> Member
mkMember time name = Member {
    memberName = name,
    memberTerm = logTerm time,
    memberLogLastAppended = time,
    memberLogLastCommitted = time 
    }

{-|
Update records of a member's state, based on the result of an RPC.
-}
updateMember :: Member -> MemberResult -> Member
updateMember member result = member {
    memberTerm = memberCurrentTerm result,
    memberLogLastAppended = memberLastAppended result,
    memberLogLastCommitted = memberLastCommitted result
    }

{-|
A collection of 'Member's.
-}
type Members = M.Map Name Member

{-|
Create a new 'Members' collection.
-}
mkMembers :: RaftConfiguration -> RaftTime -> Members
mkMembers cfg time = M.fromList $ map (\name -> (name,mkMember time name)) (clusterMembers $ clusterConfiguration cfg)

{-|
Adjust a 'Members' collection, based on a new configuration. If a 'Name' is no longer listed
as a member in the 'Configuration', then the corresponding 'Member' will no longer be
present in the 'Members' collection. If a new 'Name' is in the configuration but not present
in the 'Members' collection, then a new 'Member' will be created to track its state.
-}
reconfigureMembers :: Members -> Configuration -> RaftTime -> Members
reconfigureMembers members cfg time = 
    M.fromList $ map (\name -> case M.lookup name members of
            Nothing -> (name,mkMember time name)
            Just member -> (name,member)) (clusterMembers cfg)

{-|
The result of invoking an RPC on a member.
-}
data MemberResult = MemberResult {
    -- | 'True' if the RPC was a success, 'False' otherwise.
    memberActionSuccess :: Bool,
    -- | The leader, if any, that the member is currently following.
    memberLeader :: Maybe Name,
    -- | The member's current 'Term'.
    memberCurrentTerm :: Term,
    -- | the 'RaftTime' of the last entry appended to the member's 'Control.Consensus.Raft.Log.RaftLog'.
    memberLastAppended :: RaftTime,
    -- | the 'RaftTime' of the last entry committed in the member's 'Control.Consensus.Raft.Log.RaftLog'.
    memberLastCommitted :: RaftTime
} deriving (Eq,Show,Generic)

instance Serialize MemberResult

{-|
A collection of 'MemberResult's, keyed by member 'Name'.
-}
type MemberResults =  M.Map Name (Maybe MemberResult)

{-|
Returns true if the majority of the results were successful.
-}
majorityConsent :: MemberResults -> Bool
majorityConsent results = count >= majority
    where
        count = length $ filter successful $ M.elems results
        majority = ((M.size results) `quot` 2) + 1
        successful Nothing = False
        successful (Just result) = memberActionSuccess result

{-|
Update the state of 'Members' based on collected 'MemberResults'.
-}
updateMembers :: Members -> MemberResults -> Members
updateMembers members results = M.map applyUpdates members
    where
        applyUpdates member =
            case M.lookup (memberName member) results of
                Just (Just result) -> updateMember member result
                _ -> member

{-|
Return the highest 'Term' that has been appended to a majority of 'Member's.
-}
membersSafeAppendedTerm :: Members -> Configuration -> Term
membersSafeAppendedTerm members cfg@(Configuration _ _ _) =
    if M.null members
        then -1
        else (membersAppendedTerm members cfg) !! majority
    where
        majority = (S.size $ configurationParticipants cfg) `quot` 2
membersSafeAppendedTerm members (JointConfiguration jointOld jointNew) =
    min (membersSafeAppendedTerm members jointOld) (membersSafeAppendedTerm members jointNew)

{-|
Return the highest 'Term's that have been appended to all 'Member's.
-}
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
membersSafeAppendedIndex members cfg@(Configuration _ _ _) =
    if M.null members
        then -1
        else (membersAppendedIndex members cfg) !! majority
    where
        majority = (S.size $ configurationParticipants cfg) `quot` 2
membersSafeAppendedIndex members (JointConfiguration jointOld jointNew) =
    min (membersSafeAppendedIndex members jointOld) (membersSafeAppendedIndex members jointNew)

{-|
Return the highest 'Index'es that have been appended to the 'Data.Log.Log'
of all 'Member's.
-}
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

{-|
Return the highest 'Index'es that have been committed in the 'Data.Log.Log'
of all 'Member's.
-}
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

{-|
Return the highest 'Term' seen by all 'Members'.
-}
membersHighestTerm :: Members -> Term
membersHighestTerm members = maximum $ map memberTerm $ M.elems members
