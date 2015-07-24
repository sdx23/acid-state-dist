--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  A replication backend for acid-state that is centered around a Master-node.
  Slave-nodes connect the Master, are synchronized and get updated by the
  Master continuously.

  Running Updates on the Master works as known from acid-state, and with
  negligible delay.
  On Slaves Updates are delayed by approximately one round trip time.
  Furthermore Slaves' Updates block (and timeout) if the Master is unreachable.

-}

module Data.Acid.Centered
    (
      PortNumber
    , openMasterState
    , openMasterStateFrom
    , MasterState(..)
    , enslaveState
    , enslaveStateFrom
    , SlaveState(..)
    ) where

import Data.Acid.Centered.Master
import Data.Acid.Centered.Slave
import Data.Acid.Centered.Common

