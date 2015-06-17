{-# LANGUAGE DeriveDataTypeable, RecordWildCards, OverloadedStrings #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered.Common
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  Stuff common to Master and Slave

-}

module Data.Acid.Centered.Common
    (
      PortNumber(..)
    , debug
    ) where

type PortNumber = Int

debug :: String -> IO ()
debug = putStrLn 
