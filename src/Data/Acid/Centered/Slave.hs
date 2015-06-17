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
--todo: this should be seperate modules
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

import Data.Acid.Centered.Common

import System.ZMQ4 (Context, Socket, Req(..), Receiver, Flag(..),
                    context, term, socket, close, 
                    connect, disconnect,
                    send, receive)

import Control.Concurrent (forkIO)
import Control.Monad (forever)

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as CS
import qualified Data.ByteString.Lazy.Char8 as CSL


--------------------------------------------------------------------------------
-- TODO
--      overthink format of messages 
--          (extra packet for data? enough space in there?)
--      seperate modules
--      quit message
--      quit handler
--

data SlaveState st 
    = SlaveState { slaveLocalState :: AcidState st
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
        debug "opening enslaved state"
        -- local
        lst <- openLocalState initialState
        -- remote
        ctx <- context
        sock <- socket ctx Req
        let addr = "tcp://" ++ address ++ ":" ++ show port
        connect sock addr
        let slaveState = SlaveState { slaveLocalState = lst
                                    , slaveZmqContext = ctx
                                    , slaveZmqAddr = addr
                                    , slaveZmqSocket = sock
                                    }
        forkIO $ slaveRepHandler slaveState 
        return $ slaveToAcidState slaveState 

slaveRepHandler :: SlaveState st -> IO ()
slaveRepHandler SlaveState{..} = forever $ do
        msg <- receive slaveZmqSocket
        case decode msg of
            Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show msg
            Right mmsg -> case mmsg of
                    -- We are sent an Update to replicate.
                    DoRep r d -> replicateUpdate slaveZmqSocket r d slaveLocalState
                    -- We are requested to Quit.
                    MasterQuit -> undefined -- todo: how get a State that wasn't closed closed?
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ show mmsg

replicateUpdate :: Socket Req -> Int -> ByteString -> AcidState st -> IO ()
replicateUpdate sock rev event lst = do
        debug "Got an Update to replicate."
        -- commit it locally 
        scheduleColdUpdate lst $ decodeEvent event
        -- send reply: we're done
        sendToMaster sock $ RepDone revision
        where revision = undefined
              decodeEvent ev = case runGet safeGet ev of
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
