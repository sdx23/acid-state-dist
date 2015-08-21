{-# LANGUAGE OverloadedStrings #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered.Common
  Copyright   :  MIT

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  non-portable (uses GHC extensions)

  Stuff common to Master and Slave in Centered systems.

-}

module Data.Acid.Centered.Common
    (
      debug
    , whenM
    , waitPoll
    , waitPollN
    , crcOfState
    , Crc
    , NodeRevision
    , Revision
    , RequestID
    , PortNumber
    , SlaveMessage(..)
    , MasterMessage(..)
    , AcidException(..)
    ) where

import Data.Typeable (Typeable)
import Data.SafeCopy (safePut)

import Data.Acid.Core (Tagged, withCoreState)
import Data.Acid.Local (localCore)
import Data.Acid.Abstract (downcast)
import Data.Acid (AcidState, IsAcidic)
import Data.Acid.CRC (crc16)

import Control.Concurrent (threadDelay)

import Control.Monad (liftM, liftM2, liftM3,
                      unless, when)
import Control.Exception (Exception)

import Data.ByteString.Lazy.Char8 (ByteString)
import Data.Serialize (Serialize(..), put, get,
                       putWord8, getWord8,
                       runPutLazy)
import Data.Word (Word16)

#ifdef nodebug
#else
import System.IO (stderr, hPutStrLn)
import qualified Control.Concurrent.Lock as L
import System.IO.Unsafe (unsafePerformIO)
#endif

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

#ifdef nodebug
-- | Debugging disabled.
debug :: String -> IO ()
debug _ = return ()
#else
-- | Lock for non-interleaved debug output.
{-# NOINLINE debugLock #-}
debugLock :: L.Lock
debugLock = unsafePerformIO L.new

-- | Debugging without interleaving output from different threads.
debug :: String -> IO ()
debug = L.with debugLock . hPutStrLn stderr
#endif

-- | Internally used for killing handler threads.
data AcidException = GracefulExit
      deriving (Show, Typeable)

instance Exception AcidException

-- | Messages the Master sends to Slaves.
data MasterMessage = DoRep Revision (Maybe RequestID) (Tagged ByteString)
                   | DoSyncRep Revision (Tagged ByteString)
                   | SyncDone Crc
                   | DoCheckpoint Revision
                   | DoSyncCheckpoint Revision ByteString
                   | DoArchive Revision
                   | FullRep Revision
                   | FullRepTo Revision
                   | MayQuit
                   | MasterQuit
                  deriving (Show)

-- | Messages Slaves sends to the Master.
data SlaveMessage = NewSlave Int
                  | RepDone Int
                  | RepError
                  | ReqUpdate RequestID (Tagged ByteString)
                  | SlaveQuit
                  deriving (Show)

instance Serialize MasterMessage where
    put msg = case msg of
        DoRep r i d          -> putWord8 0 >> put r >> put i >> put d
        DoSyncRep r d        -> putWord8 1 >> put r >> put d
        SyncDone c           -> putWord8 2 >> put c
        DoCheckpoint r       -> putWord8 3 >> put r
        DoSyncCheckpoint r d -> putWord8 4 >> put r >> put d
        DoArchive r          -> putWord8 5 >> put r
        FullRep r            -> putWord8 6 >> put r
        FullRepTo r          -> putWord8 7 >> put r
        MayQuit              -> putWord8 8
        MasterQuit           -> putWord8 9
    get = do
        tag <- getWord8
        case tag of
            0 -> liftM3 DoRep get get get
            1 -> liftM2 DoSyncRep get get
            2 -> liftM SyncDone get
            3 -> liftM DoCheckpoint get
            4 -> liftM2 DoSyncCheckpoint get get
            5 -> liftM DoArchive get
            6 -> liftM FullRep get
            7 -> liftM FullRepTo get
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

-- | By polling, wait until predicate fulfilled.
waitPoll :: Int -> IO Bool -> IO ()
waitPoll t p = p >>= \e -> unless e $ threadDelay t >> waitPoll t p

-- | By polling, wait until predicate fulfilled. Poll at max. n times.
waitPollN :: Int -> Int -> IO Bool -> IO ()
waitPollN t n p
    | n == 0    = return ()
    | otherwise = p >>= \e -> unless e $ threadDelay t >> waitPollN t (n-1) p

-- | Monadic when
whenM :: Monad m => m Bool -> m () -> m ()
whenM b a = b >>= flip when a
