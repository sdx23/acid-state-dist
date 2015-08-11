--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  non-portable (uses GHC extensions)

  A replication backend for acid-state that is centered around a Master node.
  Slave nodes connect to the Master, are synchronized and get updated by the
  Master continuously. This backend offers two flavors of operation:

    [@Regular operation@]   No redundancy guarantees but fast.
    [@Redundant operation@] Guarantees redundant replication on /n/ nodes but
                            slower.

  In both cases Slaves' Updates block (and eventually time out) if the Master is
  unreachable.

  Queries operate on the local state with the performance known from acid-state.
  Note that state on all nodes is eventually consistent, i.e. it might differ
  shortly (for Queries run concurrently to Updates being serialized).

-}

module Data.Acid.Centered
    (
-- * Usage
-- |
-- Open your AcidState using one of the functions below. Afterwards the usual
-- interface of acid state is available.
--
-- Always make sure to have sensible exception management since naturally a lot
-- more error sources exist with this backend than do for a 'Data.Acid.Local'
-- AcidState.
-- Using 'Control.Exception.bracket' is recommended for achieving this
-- conveniently:
--
-- > main = bracket
-- >          (enslaveState ...)
-- >          closeAcidState
-- >          $ \acid -> do
-- >               ...
--
-- 'Data.Acid.createCheckpoint' issued on Master is a global operation,
-- while issued on a Slave it is not.
--
-- 'Data.Acid.createArchive' is an operation local to each node since usually
-- further action is required. For global Archives see 'createArchiveGlobally'.


    -- * Regular operation
    -- |
    -- Running Updates on Master works as known from acid-state and with
    -- negligible performance loss.
    -- On Slaves Updates are delayed by approximately one round trip time (RTT).
    -- When no Slaves are connected Updates are only serialized on the Master,
    -- i.e. there is no redundancy.
      openMasterState
    , openMasterStateFrom
    , enslaveState
    , enslaveStateFrom
    -- * Redundant operation
    -- |
    -- When Updates are scheduled they are sent out and written to disk on all
    -- nodes. However, an Update is visible to Queries (and its result returned)
    -- /only/ as soon as at least /n/ nodes are done replicating it. Thus each
    -- Update is delayed for at least one RTT.
    --
    -- If less than /n-1/ Slave nodes are connected, all Updates are blocked
    -- until enough nodes are available again. Queries are not affected and
    -- operate on the last /n/-replicated state.
    --
    -- /Note:/ Shutting down and restarting the Master resumes the last state
    -- including even Updates that were not /n/-replicated.
    , openRedMasterState
    , openRedMasterStateFrom
    , enslaveRedState
    , enslaveRedStateFrom
    -- * Types
    , PortNumber
    ) where

import Data.Acid.Centered.Master
import Data.Acid.Centered.Slave
import Data.Acid.Centered.Common

