{-# LANGUAGE RecordWildCards #-}

module Data.Acid.Raft (
      runRaft
    ,
)   where

import Data.Acid.Raft.Common
import Data.Acid.Raft.Roles

import Control.Monad (forM_)

-- | Start up Raft.
runRaft :: ComChan -> NodeSet -> NodeIdentity -> IO ()
runRaft cc nis ident = stepRaft cc ns
    where ns = initialNodeState ident nis

-- | Main handler routine of raft. Here IO and pure raft are separated.
stepRaft :: ComChan -> NodeState -> IO ()
stepRaft cc ns = do
    msg <- receiveMsgs cc (nTimeout ns)
    let (nns, es) = raftAction ns msg
    forM_ es $ processEnvelope cc
    -- now loop
    stepRaft cc nns

-- fixme: nodeState should not be passed around, rather StateMonad

-- fixme: rather use a queue for actions instead of envelope list, so we can put
-- in actions to replicate log entries as well

-- | Distinguish action by node role
-- fixme: disinguishing by action/message could be more sensible
raftAction :: NodeState -> Message -> (NodeState, [Envelope])
raftAction ns@NodeState{..} ms =
    case nRole of
        Follower  -> raftActFollower ns ms
        Candidate -> raftActCandidate ns ms
        Leader    -> raftActLeader ns ms

-- | A node starts as Follower.
initialNodeState :: NodeIdentity -> NodeSet -> NodeState
initialNodeState i ns = NodeState {
      nIdent = i
    , nSet = ns
    , nRole = Follower
    , nTerm = 0
    , nVotedFor = i
    , nVotesGranted = 0
    , nTimeout = raftFollowerTimeout
    }

-- fixme: use randGen in nodeState and just extract one
raftFollowerTimeout :: Int
raftFollowerTimeout = 150

--raftElectionTimeout :: Int
--raftElectionTimeout = 500

