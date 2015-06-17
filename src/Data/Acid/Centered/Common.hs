{-# LANGUAGE OverloadedStrings #-}
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
      debug
    , PortNumber(..)
    , SlaveMessage(..)
    , MasterMessage(..)
    ) where

import Control.Monad (liftM, liftM2)
import Data.ByteString.Char8 (ByteString)
import Data.Serialize (Serialize(..), put, get,
                       putWord8, getWord8,
                      )

type PortNumber = Int

debug :: String -> IO ()
debug = putStrLn 

data MasterMessage = DoRep Int ByteString
                  deriving (Show)
                -- later:
                -- | Quit

data SlaveMessage = NewSlave Int
                  | RepDone Int
                  deriving (Show)
               -- later:
               -- | Update ByteString
               -- | Quit

instance Serialize MasterMessage where
    put msg = case msg of
        DoRep r d -> putWord8 0 >> put r >> put d
    get = do 
        tag <- getWord8
        case tag of
            0 -> liftM2 DoRep get get
            _ -> error $ "Data.Serialize.get failed for MasterMessage: invalid tag " ++ show tag

instance Serialize SlaveMessage where
    put msg = case msg of
        NewSlave r -> putWord8 0 >> put r
        RepDone r  -> putWord8 1 >> put r
    get = do
        tag <- getWord8
        case tag of
            0 -> liftM NewSlave get
            1 -> liftM RepDone get
            _ -> error $ "Data.Serialize.get failed for SlaveMessage: invalid tag " ++ show tag
