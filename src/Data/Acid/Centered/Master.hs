{-# LANGUAGE DeriveDataTypeable, RecordWildCards #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered.Master
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  This module provides a replication backend for acid state, centered around 
  the master. Thus in case of partitions no updates may be accepted and the
  system blocks.

-}
{- big chunks still todo:
    o checkpoints / archives
    o authentification
    o encryption
-}
module Data.Acid.Centered.Master
    (
      openMasterState
    , MasterState(..)
    ) where

import Data.Typeable
import Data.SafeCopy

-- some not exported by acid-state, export and reinstall: Core, Abstract, Log
import Data.Acid
import Data.Acid.Core
import Data.Acid.Abstract 
import Data.Acid.Advanced
import Data.Acid.Local
import Data.Acid.Log
import Data.Serialize (Serialize(..), put, get,
                       decode, encode,
                       runPutLazy, runPut
                      )

import Data.Acid.Centered.Common

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Monad (when, unless, void,
                      forever, forM_, 
                      liftM, liftM2)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)

import System.ZMQ4 (Context, Socket, Router(..), Receiver, Flag(..),
                    setReceiveHighWM, setSendHighWM, restrict,
                    context, term, socket, close, 
                    bind, unbind,
                    poll, Poll(..), Event(..),
                    waitRead,
                    sendMulti, receiveMulti)

import qualified Data.Map as M
import Data.Map (Map)
import qualified Data.ByteString.Lazy.Char8 as CSL
import qualified Data.ByteString.Char8 as CS
import Data.ByteString.Char8 (ByteString)
import qualified Data.List.NonEmpty as NEL

-- auto imports following - need to be cleaned up
import Control.Concurrent.MVar(MVar, newMVar, newEmptyMVar,
                               takeMVar, putMVar,
                               readMVar,
                               modifyMVar, modifyMVar_, withMVar)

--------------------------------------------------------------------------------

data MasterState st 
    = MasterState { localState :: AcidState st
                  , nodeStatus :: MVar NodeStatus
                  , masterRevision :: MVar NodeRevision
                  , masterReplicationChan :: Chan ReplicationItem
                  , zmqContext :: Context
                  , zmqAddr :: String
                  , zmqSocket :: MVar (Socket Router)
                  } deriving (Typeable)

type NodeIdentity = ByteString
type NodeStatus = Map NodeIdentity NodeRevision
type Callback = IO ()
type ReplicationItem = (Tagged CSL.ByteString, Either Callback (RequestID, NodeIdentity))
        
-- | The request handler on master node. Does
--      o handle receiving requests from nodes,
--      o answering as needed (old updates),
--      o bookkeeping on node states. 
masterRequestHandler :: (IsAcidic st, Typeable st) => MasterState st -> IO ()
masterRequestHandler masterState@MasterState{..} = forever $ do
        -- take one frame
        -- waitRead =<< readMVar zmqSocket
        -- FIXME: we needn't poll if not for strange zmq behaviour
        re <- withMVar zmqSocket $ \sock -> poll 100 [Sock sock [In] Nothing]
        unless (null $ head re) $ do
            (ident, msg) <- withMVar zmqSocket receiveFrame
            -- handle according frame contents
            case msg of
                -- New Slave joined.
                NewSlave r -> do
                    pastUpdates <- getPastUpdates localState r
                    connectNode masterState ident pastUpdates
                -- Slave is done replicating.
                RepDone r -> return () -- updateNodeStatus masterState ident r
                -- Slave sends an Udate.
                ReqUpdate rid event ->
                    queueUpdate masterState (event, Right (rid, ident))
                -- Slave quits.
                SlaveQuit -> do
                    removeFromNodeStatus nodeStatus ident
                    sendToSlave zmqSocket MayQuit ident
                -- no other messages possible
                _ -> error $ "Unknown message received: " ++ show msg
            -- loop around
            debug "Loop iteration."

-- | Fetch past Updates from FileLog for replication.
getPastUpdates :: (Typeable st) => AcidState st -> Int -> IO [(Int, Tagged CSL.ByteString)]
getPastUpdates state startRev = liftM2 zip (return [(startRev+1)..]) (readEntriesFrom (localEvents $ downcast state) startRev)

-- | Remove a Slave node from NodeStatus.
removeFromNodeStatus :: MVar NodeStatus -> NodeIdentity -> IO ()
removeFromNodeStatus nodeStatus ident =
        modifyMVar_ nodeStatus $ return . M.delete ident

-- | Update the NodeStatus after a node has replicated an Update.
updateNodeStatus :: MasterState st -> NodeIdentity -> Int -> IO ()
updateNodeStatus MasterState{..} ident r = 
    modifyMVar_ nodeStatus $ \ns -> do
        -- todo: there should be a fancy way to do this
        when (M.findWithDefault 0 ident ns /= (r - 1)) $
            error $ "Invalid increment of node status " ++ show ns ++ " -> " ++ show r
        let rs = M.adjust (+1) ident ns
        return rs
        --withMVar masterRevision $ \mr -> do
        --    when (allNodesDone mr rs) $ debug $ "All nodes done replicating " ++ show mr
        --    where allNodesDone mrev = M.fold (\v t -> (v == mrev) && t) True

-- | Connect a new Slave by getting it up-to-date,
--   i.e. send all past events as Updates. This is fire&forget.
--   todo: check HWM
--   todo: check sync validity
connectNode :: (IsAcidic st, Typeable st) => MasterState st -> NodeIdentity -> [(Int, Tagged CSL.ByteString)] -> IO ()
connectNode MasterState{..} i pastUpdates = 
    withMVar masterRevision $ \mr -> 
        modifyMVar_ nodeStatus $ \ns -> do
            -- todo: do we need to lock masterState for crc?
            crc <- crcOfState localState
            forM_ pastUpdates $ \(r, u) -> sendSyncUpdate zmqSocket r u i
            sendToSlave zmqSocket (SyncDone crc) i
            return $ M.insert i mr ns 

-- | Send a message to a Slave
sendToSlave :: MVar (Socket Router) -> MasterMessage -> NodeIdentity -> IO ()
sendToSlave msock msg ident = withMVar msock $ \sock -> sendMulti sock $ NEL.fromList [ident, encode msg]

-- | Send one (encoded) Update to a Slave.
sendSyncUpdate :: MVar (Socket Router) -> Revision -> Tagged CSL.ByteString -> NodeIdentity -> IO ()
sendSyncUpdate sock revision update = sendToSlave sock (DoSyncRep revision update) 
    
-- | Send one (encoded) Update to a Slave.
sendUpdate :: MVar (Socket Router) -> Revision -> Maybe RequestID -> Tagged CSL.ByteString -> NodeIdentity -> IO ()
sendUpdate sock revision reqId update = sendToSlave sock (DoRep revision reqId update) 
    
-- | Receive one Frame. A Frame consists of three messages: 
--      sender ID, empty message, and actual content 
receiveFrame :: (Receiver t) => Socket t -> IO (NodeIdentity, SlaveMessage)
receiveFrame sock = do
    list <- receiveMulti sock
    when (length list /= 2) $ error "Received invalid frame."
    let ident = head list
    let msg = list !! 1
    case decode msg of
        -- todo: pass on exceptions
        Left str -> error $ "Data.Serialize.decode failed on SlaveMessage: " ++ show msg
        Right smsg -> do
            debug $ "Received from [" ++ CS.unpack ident ++ "]: "
                        ++ take 20 (show smsg)
            return (ident, smsg)

-- | Open the master state.
openMasterState :: (IsAcidic st, Typeable st) =>
               PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state 
            -> IO (AcidState st)
openMasterState port initialState = do
        debug "opening master state"
        -- local
        lst <- openLocalState initialState
        let levs = localEvents $ downcast lst
        lrev <- atomically $ readTVar $ logNextEntryId levs
        rev <- newMVar lrev
        repChan <- newChan
        ns <- newMVar M.empty
        -- remote
        let addr = "tcp://127.0.0.1:" ++ show port
        ctx <- context
        sock <- socket ctx Router
        setReceiveHighWM (restrict (100*1000)) sock
        setSendHighWM (restrict (100*1000)) sock
        bind sock addr
        msock <- newMVar sock
        let masterState = MasterState { localState = lst
                                      , nodeStatus = ns
                                      , masterRevision = rev
                                      , masterReplicationChan = repChan
                                      , zmqContext = ctx
                                      , zmqAddr = addr
                                      , zmqSocket = msock
                                      }
        forkIO $ masterRequestHandler masterState
        forkIO $ masterReplicationHandler masterState
        return $ toAcidState masterState

-- | Close the master state.
closeMasterState :: MasterState st -> IO ()
closeMasterState MasterState{..} = do
        debug "Closing master state."
        -- wait all nodes done
        -- todo^ - not necessary for now
        -- cleanup zmq
        withMVar zmqSocket $ \sock -> do
            unbind sock zmqAddr 
            close sock
        term zmqContext
        -- cleanup local state
        closeAcidState localState

-- | Update on master site.
-- todo: this implementation is only valid for Slaves not sending Updates.
scheduleMasterUpdate :: UpdateEvent event => MasterState (EventState event) -> event -> IO (MVar (EventResult event))
scheduleMasterUpdate masterState@MasterState{..} event = do
        debug "Update by Master."
        result <- newEmptyMVar 
        let callback = do
                hd <- scheduleUpdate localState event
                void $ forkIO (putMVar result =<< takeMVar hd)
        let encoded = runPutLazy (safePut event) 
        queueUpdate masterState ((methodTag event, encoded), Left callback)
        return result
             
-- | Remove nodes that were not responsive
removeLaggingNodes :: MasterState st -> IO ()
removeLaggingNodes MasterState{..} = 
    -- todo: send the node a quit notice
    withMVar masterRevision $ \mr -> modifyMVar_ nodeStatus $ return . M.filter (== mr) 

-- | Queue an Update (originating from the Master itself of an Slave via zmq)
queueUpdate :: MasterState st -> ReplicationItem -> IO ()
queueUpdate MasterState{..} = writeChan masterReplicationChan

-- | The replication handler. Takes care to run Updates locally in the same
--   order as sending them out to the network.
masterReplicationHandler :: MasterState st -> IO ()
masterReplicationHandler MasterState{..} = forever $ do
        debug "Replicating next item."
        (event, sink) <- readChan masterReplicationChan
        -- todo: temporary only one Chan, no improvement to without chan!
        -- local part
        withMVar masterRevision $ \mr -> debug $ "Replicating Update myself to " ++ show (mr + 1)
        case sink of 
            Left callback   -> callback 
            _               -> void $ scheduleColdUpdate localState event
        -- remote part
        withMVar nodeStatus $ \ns -> do
            debug $ "Sending Update to Slaves, there are " ++ show (M.size ns)
            modifyMVar_ masterRevision $ \mrOld -> do
                let mr = mrOld + 1 
                case sink of
                    Left _ -> forM_ (M.keys ns) $ sendUpdate zmqSocket mr Nothing event 
                    Right (reqID, reqNodeIdent) -> do
                        let noReqSlaves = filter (/= reqNodeIdent) $ M.keys ns 
                        sendUpdate zmqSocket mr (Just reqID) event reqNodeIdent
                        forM_ noReqSlaves $ sendUpdate zmqSocket mr Nothing event 
                return mr

toAcidState :: IsAcidic st => MasterState st -> AcidState st
toAcidState master 
  = AcidState { _scheduleUpdate    = scheduleMasterUpdate master 
              , scheduleColdUpdate = scheduleColdUpdate $ localState master
              , _query             = query $ localState master
              , queryCold          = queryCold $ localState master
              , createCheckpoint   = undefined
              , createArchive      = undefined
              , closeAcidState     = closeMasterState master 
              , acidSubState       = mkAnyState master
              }


