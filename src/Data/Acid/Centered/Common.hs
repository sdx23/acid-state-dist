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
    , NodeRevision
    , PortNumber(..)
    , SlaveMessage(..)
    , MasterMessage(..)
    ) where

--import Data.Acid.Core (Tagged(..))

import Control.Monad (liftM, liftM2)
import Data.ByteString.Char8 (ByteString)
import Data.Serialize (Serialize(..), put, get,
                       putWord8, getWord8,
                      )

type PortNumber = Int

type NodeRevision = Int

debug :: String -> IO ()
debug = putStrLn 

data MasterMessage = DoRep Int ByteString
                   | MasterQuit
                  deriving (Show)

data SlaveMessage = NewSlave Int
                  | RepDone Int
                  | RepError
                  | SlaveQuit
                  deriving (Show)
               -- todo, later:
               -- | Update ByteString

instance Serialize MasterMessage where
    put msg = case msg of
        DoRep r d -> putWord8 0 >> put r >> put d
        MasterQuit -> putWord8 9
    get = do 
        tag <- getWord8
        case tag of
            0 -> liftM2 DoRep get get
            9 -> return MasterQuit
            _ -> error $ "Data.Serialize.get failed for MasterMessage: invalid tag " ++ show tag

instance Serialize SlaveMessage where
    put msg = case msg of
        NewSlave r -> putWord8 0 >> put r
        RepDone r  -> putWord8 1 >> put r
        RepError   -> putWord8 2
        SlaveQuit  -> putWord8 9
    get = do
        tag <- getWord8
        case tag of
            0 -> liftM NewSlave get
            1 -> liftM RepDone get
            2 -> return RepError
            9 -> return SlaveQuit
            _ -> error $ "Data.Serialize.get failed for SlaveMessage: invalid tag " ++ show tag
