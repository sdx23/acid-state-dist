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
    , Revision
    , RequestID
    , PortNumber(..)
    , SlaveMessage(..)
    , MasterMessage(..)
    ) where

import Data.Acid.Core (Tagged(..))

import Control.Monad (liftM, liftM2, liftM3)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as CSL
import Data.Serialize (Serialize(..), put, get,
                       putWord8, getWord8,
                      )
import System.IO (stderr, hPutStrLn)

--------------------------------------------------------------------------------

-- | Number of a port for establishing a network connection.
type PortNumber = Int

-- | (Current) Revision of a node.
type NodeRevision = Int

-- | Revision an Update resembles.
type Revision = Int

-- | ID of an Update Request.
type RequestID = Int

debug :: String -> IO ()
debug = hPutStrLn stderr

data MasterMessage = DoRep Revision (Maybe RequestID) (Tagged CSL.ByteString)
                   | DoSyncRep Revision (Tagged CSL.ByteString)
                   | SyncDone
                   | MasterQuit
                  deriving (Show)

data SlaveMessage = NewSlave Int
                  | RepDone Int
                  | RepError
                  | ReqUpdate RequestID (Tagged CSL.ByteString)
                  | SlaveQuit
                  deriving (Show)

instance Serialize MasterMessage where
    put msg = case msg of
        DoRep r i d   -> putWord8 0 >> put r >> put i >> put d
        DoSyncRep r d -> putWord8 1 >> put r >> put d
        SyncDone      -> putWord8 2
        MasterQuit    -> putWord8 9
    get = do 
        tag <- getWord8
        case tag of
            0 -> liftM3 DoRep get get get
            1 -> liftM2 DoSyncRep get get
            2 -> return SyncDone
            9 -> return MasterQuit
            _ -> error $ "Data.Serialize.get failed for MasterMessage: invalid tag " ++ show tag

instance Serialize SlaveMessage where
    put msg = case msg of
        NewSlave r    -> putWord8 0 >> put r
        RepDone r     -> putWord8 1 >> put r
        RepError      -> putWord8 2
        ReqUpdate i d -> putWord8 3 >> put i >> put d
        SlaveQuit     -> putWord8 9
    get = do
        tag <- getWord8
        case tag of
            0 -> liftM NewSlave get
            1 -> liftM RepDone get
            2 -> return RepError
            3 -> liftM2 ReqUpdate get get
            9 -> return SlaveQuit
            _ -> error $ "Data.Serialize.get failed for SlaveMessage: invalid tag " ++ show tag
