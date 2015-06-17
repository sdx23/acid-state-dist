{-# LANGUAGE DeriveDataTypeable, RecordWildCards, OverloadedStrings #-}
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
      PortNumber(..)
    , openMasterState
    , MasterState(..)
    , enslaveState
    , SlaveState(..)
    ) where

import Data.Acid.Centered.Master
import Data.Acid.Centered.Slave
import Data.Acid.Centered.Common

