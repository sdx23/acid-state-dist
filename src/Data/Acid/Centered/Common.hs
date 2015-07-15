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
    , waitPoll
    , crcOfState
    , Crc
    , NodeRevision
    , Revision
    , RequestID
    , PortNumber(..)
    , SlaveMessage(..)
    , MasterMessage(..)
    ) where

import Data.Acid.Core (Tagged(..), withCoreState)
import Data.Acid.Local (localCore)
import Data.Acid.Abstract (downcast)
import Data.Acid (AcidState, IsAcidic)
import Data.Acid.CRC (crc16)

import Control.Monad (liftM, liftM2, liftM3,
                      unless
                     )
import Control.Concurrent (ThreadId, myThreadId, threadDelay)
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as CSL
import Data.Serialize (Serialize(..), put, get,
                       putWord8, getWord8,
                       runPutLazy
                      )
import Data.Typeable (Typeable)
import Data.SafeCopy (safePut)
import Data.Word (Word16)
import System.IO (stderr, hPutStrLn)
import qualified Control.Concurrent.Lock as L
import System.IO.Unsafe (unsafePerformIO)

--------------------------------------------------------------------------------

-- | Number of a port for establishing a network connection.
type PortNumber = Int

-- | (Current) Revision of a node.
type NodeRevision = Int

-- | Revision an Update resembles.
type Revision = Int

-- | ID of an Update Request.
type RequestID = Int

-- | We use CRC16 for now.
type Crc = Word16


-- | Debugging without interleaving output from different threads
{-# NOINLINE debugLock #-}
debugLock :: L.Lock
debugLock = unsafePerformIO L.new

debug :: String -> IO ()
debug = L.with debugLock . hPutStrLn stderr
-- to turn off debug use
--debug _ = return ()

data MasterMessage = DoRep Revision (Maybe RequestID) (Tagged CSL.ByteString)
                   | DoSyncRep Revision (Tagged CSL.ByteString)
                   | SyncDone Crc
                   | MayQuit
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
        SyncDone c    -> putWord8 2 >> put c
        MayQuit       -> putWord8 8
        MasterQuit    -> putWord8 9
    get = do 
        tag <- getWord8
        case tag of
            0 -> liftM3 DoRep get get get
            1 -> liftM2 DoSyncRep get get
            2 -> liftM SyncDone get
            8 -> return MayQuit
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

-- | Compute the CRC of a state.
crcOfState :: (IsAcidic st, Typeable st) => AcidState st -> IO Crc
crcOfState state = do
    let lst = downcast state
    withCoreState (localCore lst) $ \st -> do
        let encoded = runPutLazy (safePut st)
        return $ crc16 encoded

-- | By polling, wait until predicate true.
waitPoll :: Int -> IO Bool -> IO ()
waitPoll t p = p >>= \e -> unless e $ threadDelay t >> waitPoll t p
