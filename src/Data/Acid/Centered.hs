--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  Centered acid state replication backend

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

