{-# LANGUAGE DeriveDataTypeable, RecordWildCards #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.CenteredSlave.hs
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  This module provides the slave part of a replication backend for acid state,
  centered around the master. Thus in case of partitions no updates may be
  accepted and the system blocks.

-}

--------------------------------------------------------------------------------
-- SLAVE part
--
-- What does a Slave do?
--      open its localState
--      check at which revision it is
--      request to be updated
--
--      do Queries locally
--      deny Updates (for now)
--      receive messages from master and respond
--      
--      notify master he's out, close local

module Data.Acid.Centered.Slave
    (
      enslaveState
    , SlaveState(..)
    )	where

import Data.Typeable
import Data.SafeCopy
import Data.Serialize (Serialize(..), put, get,
                       decode, encode,
                       runPutLazy, runPut,
                       runGet
                      )

import Data.Acid
import Data.Acid.Core
import Data.Acid.Abstract
import Data.Acid.Local
import Data.Acid.Log

import Data.Acid.Centered.Common

import System.ZMQ4 (Context, Socket, Req(..), Receiver, Flag(..),
                    context, term, socket, close, 
                    connect, disconnect,
                    send, receive)

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newMVar, modifyMVar_)
import Control.Monad (forever)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as CS
import qualified Data.ByteString.Lazy.Char8 as CSL

--------------------------------------------------------------------------------

data SlaveState st 
    = SlaveState { slaveLocalState :: AcidState st
                 , slaveRevision :: MVar NodeRevision
                 , slaveZmqContext :: Context
                 , slaveZmqAddr :: String
                 , slaveZmqSocket :: Socket Req
                 } deriving (Typeable)

-- | Open a local State as Slave for a Master.
enslaveState :: (IsAcidic st, Typeable st) =>
            String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveState address port initialState = do
        -- local
        lst <- openLocalState initialState
        let levs = localEvents $ downcast lst
        lrev <- atomically $ readTVar $ logNextEntryId levs
        rev <- newMVar lrev
        -- remote
        debug $ "Opening enslaved state at revision " ++ show lrev
        ctx <- context
        sock <- socket ctx Req
        let addr = "tcp://" ++ address ++ ":" ++ show port
        connect sock addr
        sendToMaster sock $ NewSlave lrev
        let slaveState = SlaveState { slaveLocalState = lst
                                    , slaveRevision = rev
                                    , slaveZmqContext = ctx
                                    , slaveZmqAddr = addr
                                    , slaveZmqSocket = sock
                                    }
        forkIO $ slaveRepHandler slaveState 
        return $ slaveToAcidState slaveState 

-- | Replication handler of the Slave. Forked and running in background all the
--   time.
slaveRepHandler :: SlaveState st -> IO ()
slaveRepHandler SlaveState{..} = forever $ do
        msg <- receive slaveZmqSocket
        case decode msg of
            Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show msg
            Right mmsg -> case mmsg of
                    -- We are sent an Update to replicate.
                    DoRep r d -> replicateUpdate slaveZmqSocket r d slaveLocalState slaveRevision
                    -- We are requested to Quit.
                    MasterQuit -> undefined -- todo: how get a State that wasn't closed closed?
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ show mmsg

replicateUpdate :: Socket Req -> Int -> Tagged CSL.ByteString -> AcidState st -> MVar NodeRevision -> IO ()
replicateUpdate sock rev event lst nrev = do
        debug $ "Got an Update to replicate " ++ show rev
        modifyMVar_ nrev $ \nr -> case rev - 1 of
            nr -> do
                -- commit it locally 
                scheduleColdUpdate lst event
                -- send reply: we're done
                sendToMaster sock $ RepDone rev
                return rev
            _  -> do 
                sendToMaster sock RepError
                error $ "Replication failed at revision " ++ show rev ++ " -> " ++ show nr
                return nr
            where decodeEvent ev = case runGet safeGet ev of
                                Left str -> error str
                                Right val -> val
    
-- | Send a message to Master.
sendToMaster :: Socket Req -> SlaveMessage -> IO ()
sendToMaster sock smsg = send sock [] $ encode smsg

-- | Close an enslaved State.
liberateState :: SlaveState st -> IO ()
liberateState SlaveState{..} = do
        debug "Closing Slave state."
        -- send master quit message
        sendToMaster slaveZmqSocket SlaveQuit
        -- cleanup zmq
        disconnect slaveZmqSocket slaveZmqAddr 
        close slaveZmqSocket
        term slaveZmqContext
        -- cleanup local state
        closeAcidState slaveLocalState


slaveToAcidState :: IsAcidic st => SlaveState st -> AcidState st
slaveToAcidState slave 
  = AcidState { _scheduleUpdate    = undefined
              , scheduleColdUpdate = undefined
              , _query             = query $ slaveLocalState slave
              , queryCold          = queryCold $ slaveLocalState slave
              , createCheckpoint   = undefined
              , createArchive      = undefined
              , closeAcidState     = liberateState slave 
              , acidSubState       = mkAnyState slave
              }
