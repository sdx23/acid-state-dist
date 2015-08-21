{-# LANGUAGE RecordWildCards #-}

module Data.Acid.Raft.Roles
    where

import Data.Acid.Raft.Common

import qualified Data.ByteString as BS

----------------------------------------------------------------------
-- | Candidate behaviour.
raftActCandidate :: NodeState -> Message -> (NodeState, [Envelope])
raftActCandidate ns Timeout = restartElection ns
raftActCandidate ns@NodeState{..} m@(VoteRequest t i)
    | t > nTerm = -- grant vote as follower
        raftActFollower ns{nRole = Follower, nTerm = t, nVotedFor = BS.empty} m
    | otherwise = -- don't grant vote
        (ns, [Receiver i (VoteResult t False)])
raftActCandidate ns@NodeState{..} (VoteResult t g)
    | t > nTerm = error "This can't happen."
        -- (ns{nRole = Follower, nTerm = t, nVotedFor = BS.empty},[])
    | t == nTerm && not g =
        (ns, [])
    | t == nTerm && g =
        if nVotesGranted > quorum then
            -- become leader
            raftActLeader ns{nRole = Leader} Timeout
        else
            (ns{nVotesGranted = nVotesGranted + 1}, [])
    | otherwise = (ns, [])
    where
        quorum = div (length nSet) 2
raftActCandidate ns@NodeState{..} (Heartbeat t)
    | t > nTerm = -- become Follower
        (ns{nRole = Follower, nTerm = t, nVotedFor = BS.empty}, [])
    | otherwise = -- all fine
        (ns, [])

----------------------------------------------------------------------
-- | Follower behaviour.
raftActFollower :: NodeState -> Message -> (NodeState, [Envelope])
raftActFollower ns Timeout = startElection ns
raftActFollower ns@NodeState{..} (VoteResult t _)
    | t > nTerm = error "This can't happen."
    | otherwise = (ns, [])
raftActFollower ns@NodeState{..} (VoteRequest t i)
    | t > nTerm = -- grant vote
        (ns{nTerm = t, nVotedFor = i}, [Receiver i (VoteResult t True)])
    | t == nTerm =
        if nVotedFor == i then
            (ns, [Receiver i (VoteResult t True)])
        else
            (ns, [Receiver i (VoteResult t False)])
    | otherwise =
            (ns, [Receiver i (VoteResult t False)])
raftActFollower ns@NodeState{..} (Heartbeat t)
    | t > nTerm = -- change term
        (ns{nTerm = t, nVotedFor = BS.empty}, [])
    | otherwise = -- all fine
        (ns, [])

----------------------------------------------------------------------
-- | Leader behaviour.
raftActLeader :: NodeState -> Message -> (NodeState, [Envelope])
raftActLeader ns@NodeState{..} Timeout = -- we send an heartbeat
    (ns, [Broadcast (Heartbeat nTerm)])
raftActLeader ns@NodeState{..} (VoteResult t _)
    | t > nTerm = error "This can't happen."
    | otherwise = (ns, [])
raftActLeader ns@NodeState{..} m@(VoteRequest t i)
    | t > nTerm =
        raftActFollower ns{nRole = Follower, nTerm = t, nVotedFor = BS.empty} m
    | t == nTerm =
        -- don't grant vote
        (ns, [Receiver i (VoteResult t False)])
    | otherwise =
        (ns, [Receiver i (VoteResult t False)])
raftActLeader _ (Heartbeat _) = error "This can't happen."

----------------------------------------------------------------------
-- General stuff

-- | Restarting an election means we're Candidate and timed out.
--   Actually the same as starting an election.
restartElection :: NodeState -> (NodeState, [Envelope])
restartElection = startElection

-- | Start an election by
--      o become Candidate
--      o setup Timervalue
--      o vote for self
--      o send out VoteRequests
startElection :: NodeState -> (NodeState, [Envelope])
startElection ns@NodeState{..} =
    (ns{nRole = Candidate, nTerm = t, nVotedFor = nIdent, nVotesGranted = 1},
        [Broadcast (VoteRequest t nIdent)])
    where t = nTerm + 1


