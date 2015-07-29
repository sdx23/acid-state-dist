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
    o authentification
    o encryption
-}
module Data.Acid.Centered.Master
    (
      openMasterState
    , openMasterStateFrom
    , createArchiveGlobally
    , MasterState(..)
    ) where

import Data.Typeable
import Data.SafeCopy

import Data.Acid
import Data.Acid.Core
import Data.Acid.Abstract
import Data.Acid.Local
import Data.Acid.Log
import Data.Serialize (decode, encode, runPutLazy)

import Data.Acid.Centered.Common

import Control.Concurrent (forkIO, ThreadId, myThreadId, killThread, throwTo)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan, dupChan)
import Control.Monad (when, unless, void, forM_, liftM2)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)
import Control.Concurrent.MVar(MVar, newMVar, newEmptyMVar,
                               takeMVar, putMVar, isEmptyMVar,
                               modifyMVar_, withMVar)
import Control.Exception (handle, throw, SomeException, AsyncException(..))

import System.ZMQ4 (Context, Socket, Router(..), Receiver,
                    setReceiveHighWM, setSendHighWM, restrict,
                    context, term, socket, close, bind, unbind,
                    poll, Poll(..), Event(..),
                    sendMulti, receiveMulti)
import System.FilePath ( (</>) )

import qualified Data.Map as M
import Data.Map (Map)
import qualified Data.ByteString.Lazy.Char8 as CSL
import qualified Data.ByteString.Char8 as CS
import Data.ByteString.Char8 (ByteString)
import qualified Data.List.NonEmpty as NEL
import Safe (headDef)

--------------------------------------------------------------------------------

-- | Master state structure, for internal use.
data MasterState st
    = MasterState { localState :: AcidState st
                  , nodeStatus :: MVar NodeStatus
                  , masterStateLock :: MVar ()
                  , masterRevision :: MVar NodeRevision
                  , masterReplicationChan :: Chan ReplicationItem
                  , masterReplicationChanN :: Chan ReplicationItem
                  , masterReqThreadId :: MVar ThreadId
                  , masterRepLThreadId :: MVar ThreadId
                  , masterRepNThreadId :: MVar ThreadId
                  , masterParentThreadId :: ThreadId
                  , zmqContext :: Context
                  , zmqAddr :: String
                  , zmqSocket :: MVar (Socket Router)
                  } deriving (Typeable)

type NodeIdentity = ByteString
type NodeStatus = Map NodeIdentity NodeRevision
type Callback = IO ()
data ReplicationItem =
      RIEnd
    | RICheckpoint
    | RIArchive
    | RIUpdate (Tagged CSL.ByteString) (Either Callback (RequestID, NodeIdentity))

-- | The request handler on master node. Does
--      o handle receiving requests from nodes,
--      o answering as needed (old updates),
--      o bookkeeping on node states.
masterRequestHandler :: (IsAcidic st, Typeable st) => MasterState st -> IO ()
masterRequestHandler masterState@MasterState{..} = do
    mtid <- myThreadId
    putMVar masterReqThreadId mtid
    let loop = handle (\e -> throwTo masterParentThreadId (e :: SomeException)) $
          handle killHandler $ do
            -- take one frame
            -- waitRead =<< readMVar zmqSocket
            -- FIXME: we needn't poll if not for strange zmq behaviour
            re <- withMVar zmqSocket $ \sock -> poll 100 [Sock sock [In] Nothing]
            unless (null $ head re) $ do
                (ident, msg) <- withMVar zmqSocket receiveFrame
                -- handle according frame contents
                case msg of
                    -- New Slave joined.
                    NewSlave r -> connectNode masterState ident r
                    -- Slave is done replicating.
                    RepDone _ -> return () -- updateNodeStatus masterState ident r, TODO
                    -- Slave sends an Udate.
                    ReqUpdate rid event ->
                        queueRepItem masterState (RIUpdate event (Right (rid, ident)))
                    -- Slave quits.
                    SlaveQuit -> do
                        sendToSlave zmqSocket MayQuit ident
                        removeFromNodeStatus nodeStatus ident
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ show msg
            loop
    loop
    where
        -- FIXME: actually we'd like our own exception for graceful exit
        killHandler :: AsyncException -> IO ()
        killHandler ThreadKilled = return ()
        killHandler e = throw e

-- | Remove a Slave node from NodeStatus.
removeFromNodeStatus :: MVar NodeStatus -> NodeIdentity -> IO ()
removeFromNodeStatus nodeStatus ident =
        modifyMVar_ nodeStatus $ return . M.delete ident

-- | Update the NodeStatus after a node has replicated an Update.
updateNodeStatus :: MasterState st -> NodeIdentity -> Int -> IO ()
updateNodeStatus MasterState{..} ident r =
    modifyMVar_ nodeStatus $ \ns -> do
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
connectNode :: (IsAcidic st, Typeable st) => MasterState st -> NodeIdentity -> Revision -> IO ()
connectNode MasterState{..} i revision =
    withMVar masterRevision $ \mr ->
        modifyMVar_ nodeStatus $ \ns -> do
            -- todo: do we need to lock masterState for crc?
            crc <- crcOfState localState
            -- if there has been one/more checkpoint in between:
            lastCp <- getLastCheckpointRev localState
            let lastCpRev = cpRevision lastCp
            debug $ "Found checkpoint at revision " ++ show lastCpRev
            if lastCpRev > revision then do
                -- send last checkpoint and events from after then
                sendSyncCheckpoint zmqSocket lastCp i
                pastUpdates <- getPastUpdates localState lastCpRev
                forM_ pastUpdates $ \(r, u) -> sendSyncUpdate zmqSocket r u i
            else do
                -- just the events
                pastUpdates <- getPastUpdates localState revision
                forM_ pastUpdates $ \(r, u) -> sendSyncUpdate zmqSocket r u i
            sendToSlave zmqSocket (SyncDone crc) i
            return $ M.insert i mr ns
    where cpRevision (Checkpoint r _) = r

-- | Fetch past Updates from FileLog for replication.
getPastUpdates :: (Typeable st) => AcidState st -> Int -> IO [(Int, Tagged CSL.ByteString)]
getPastUpdates state startRev = liftM2 zip (return [(startRev+1)..]) (readEntriesFrom (localEvents $ downcast state) startRev)

-- | Get the revision at which the last checkpoint was taken.
getLastCheckpointRev :: (Typeable st) => AcidState st -> IO Checkpoint
getLastCheckpointRev state = do
    let lst = downcast state
    let cplog = localCheckpoints lst
    nextId <- atomically $ readTVar $ logNextEntryId cplog
    cps <- readEntriesFrom cplog (nextId - 1)
    return $ headDef (Checkpoint 0 CSL.empty) cps

-- | Send a message to a Slave
sendToSlave :: MVar (Socket Router) -> MasterMessage -> NodeIdentity -> IO ()
sendToSlave msock msg ident = withMVar msock $ \sock -> sendMulti sock $ NEL.fromList [ident, encode msg]

-- | Send an encoded Checkpoint to a Slave.
sendSyncCheckpoint :: MVar (Socket Router) -> Checkpoint -> NodeIdentity -> IO ()
sendSyncCheckpoint sock (Checkpoint cr encoded) =
    sendToSlave sock (DoSyncCheckpoint cr encoded)

-- | Send one (encoded) Update to a Slave.
sendSyncUpdate :: MVar (Socket Router) -> Revision -> Tagged CSL.ByteString -> NodeIdentity -> IO ()
sendSyncUpdate sock revision encoded = sendToSlave sock (DoSyncRep revision encoded)

-- | Send one (encoded) Update to a Slave.
sendUpdate :: MVar (Socket Router) -> Revision -> Maybe RequestID -> Tagged CSL.ByteString -> NodeIdentity -> IO ()
sendUpdate sock revision reqId encoded = sendToSlave sock (DoRep revision reqId encoded)

sendCheckpoint :: MVar (Socket Router) -> Revision -> NodeIdentity -> IO ()
sendCheckpoint sock revision = sendToSlave sock (DoCheckpoint revision)

sendArchive :: MVar (Socket Router) -> Revision -> NodeIdentity -> IO ()
sendArchive sock revision = sendToSlave sock (DoArchive revision)

-- | Receive one Frame. A Frame consists of three messages:
--      sender ID, empty message, and actual content
receiveFrame :: (Receiver t) => Socket t -> IO (NodeIdentity, SlaveMessage)
receiveFrame sock = do
    list <- receiveMulti sock
    when (length list /= 2) $ error "Received invalid frame."
    let ident = head list
    let msg = list !! 1
    case decode msg of
        Left str -> error $ "Data.Serialize.decode failed on SlaveMessage: " ++ show str
        Right smsg -> do
            debug $ "Received from [" ++ CS.unpack ident ++ "]: "
                        ++ take 20 (show smsg)
            return (ident, smsg)

-- | Open the master state. The directory for the local state files is the
-- default one ("state/NameOfState").
openMasterState :: (IsAcidic st, Typeable st) =>
               String       -- ^ address to bind (useful to listen on specific interfaces only)
            -> PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state
            -> IO (AcidState st)
openMasterState address port initialState =
    openMasterStateFrom ("state" </> show (typeOf initialState)) address port initialState

-- | Open the master state from a specific location.
openMasterStateFrom :: (IsAcidic st, Typeable st) =>
               FilePath     -- ^ location of the local state files
            -> String       -- ^ address to bind (useful to listen on specific interfaces only)
            -> PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state
            -> IO (AcidState st)
openMasterStateFrom directory address port initialState = do
        debug "opening master state"
        -- local
        lst <- openLocalStateFrom directory initialState
        let levs = localEvents $ downcast lst
        lrev <- atomically $ readTVar $ logNextEntryId levs
        rev <- newMVar lrev
        repChan <- newChan
        repChanN <- dupChan repChan
        ns <- newMVar M.empty
        -- remote
        let addr = "tcp://" ++ address ++ ":" ++ show port
        ctx <- context
        sock <- socket ctx Router
        setReceiveHighWM (restrict (100*1000 :: Int)) sock
        setSendHighWM (restrict (100*1000 :: Int)) sock
        bind sock addr
        msock <- newMVar sock
        repTidL <- newEmptyMVar
        repTidN <- newEmptyMVar
        reqTid <- newEmptyMVar
        parTid <- myThreadId
        lock <- newEmptyMVar
        let masterState = MasterState { localState = lst
                                      , nodeStatus = ns
                                      , masterStateLock = lock
                                      , masterRevision = rev
                                      , masterReplicationChan = repChan
                                      , masterReplicationChanN = repChanN
                                      , masterRepLThreadId = repTidL
                                      , masterRepNThreadId = repTidN
                                      , masterReqThreadId = reqTid
                                      , masterParentThreadId = parTid
                                      , zmqContext = ctx
                                      , zmqAddr = addr
                                      , zmqSocket = msock
                                      }
        void $ forkIO $ masterRequestHandler masterState
        void $ forkIO $ masterReplicationHandlerL masterState
        void $ forkIO $ masterReplicationHandlerN masterState
        return $ toAcidState masterState

-- | Close the master state.
closeMasterState :: MasterState st -> IO ()
closeMasterState MasterState{..} = do
        debug "Closing master state."
        -- disallow requests
        putMVar masterStateLock ()
        -- send nodes quit
        debug "Nodes to quitting."
        withMVar nodeStatus $ mapM_ (sendToSlave zmqSocket MasterQuit) . M.keys
        -- wait all nodes done
        waitPoll 100 (withMVar nodeStatus (return . M.null))
        -- todo: this could use a timeout, there may be zombies
        -- wait replication chan
        debug "Waiting for repChans to empty."
        writeChan masterReplicationChan RIEnd
        mtid <- myThreadId
        putMVar masterRepLThreadId mtid
        putMVar masterRepNThreadId mtid
        -- kill handler
        debug "Killing request handler."
        withMVar masterReqThreadId killThread
        -- cleanup zmq
        debug "Closing down zmq."
        withMVar zmqSocket $ \sock -> do
            unbind sock zmqAddr
            close sock
        term zmqContext
        -- cleanup local state
        closeAcidState localState

-- | Update on master site.
scheduleMasterUpdate :: UpdateEvent event => MasterState (EventState event) -> event -> IO (MVar (EventResult event))
scheduleMasterUpdate masterState@MasterState{..} event = do
        debug "Update by Master."
        unlocked <- isEmptyMVar masterStateLock
        if not unlocked then error "State is locked!"
        else do
            result <- newEmptyMVar
            let callback = do
                    hd <- scheduleUpdate localState event
                    void $ forkIO (putMVar result =<< takeMVar hd)
            let encoded = runPutLazy (safePut event)
            queueRepItem masterState (RIUpdate (methodTag event, encoded) (Left callback))
            return result

-- | Queue an RepItem (originating from the Master itself of an Slave via zmq)
queueRepItem :: MasterState st -> ReplicationItem -> IO ()
queueRepItem MasterState{..} = writeChan masterReplicationChan

-- | The local replication handler. Takes care to run Updates locally.
masterReplicationHandlerL :: MasterState st -> IO ()
masterReplicationHandlerL MasterState{..} = do
    mtid <- myThreadId
    putMVar masterRepLThreadId mtid
    let loop = handle (\e -> throwTo masterParentThreadId (e :: SomeException)) $ do
            debug "Replicating next item locally."
            repItem <- readChan masterReplicationChan
            case repItem of
                RIEnd -> return ()
                RIArchive -> do
                    debug "Archive on master."
                    createArchive localState
                    loop
                RICheckpoint -> do
                    debug "Checkpoint on master."
                    createCheckpoint localState
                    loop
                RIUpdate event sink -> do
                    -- todo: this is not the acutual update number, since
                    -- splitting up the chan
                    -- also: be careful, this kills performance
                    --withMVar masterRevision $ \mr ->
                    --    debug $ "Replicating Update myself to " ++ show (mr + 1)
                    case sink of
                        Left callback   -> callback
                        _               -> void $ scheduleColdUpdate localState event
                    loop
    loop
    -- signal that we're done
    void $ takeMVar masterRepLThreadId

-- | The network replication handler. Takes care to run Updates on Slaves.
masterReplicationHandlerN :: MasterState st -> IO ()
masterReplicationHandlerN MasterState{..} = do
    mtid <- myThreadId
    putMVar masterRepNThreadId mtid
    let loop = handle (\e -> throwTo masterParentThreadId (e :: SomeException)) $ do
            debug "Replicating next item in network."
            repItem <- readChan masterReplicationChanN
            case repItem of
                RIEnd -> return ()
                RIArchive -> do
                    withMVar nodeStatus $ \ns -> do
                        debug "Sending archive request to Slaves."
                        withMVar masterRevision $ \mr ->
                            forM_ (M.keys ns) $ sendArchive zmqSocket mr
                    loop
                RICheckpoint -> do
                    withMVar nodeStatus $ \ns -> do
                        debug "Sending Checkpoint Request to Slaves."
                        withMVar masterRevision $ \mr ->
                            forM_ (M.keys ns) $ sendCheckpoint zmqSocket mr
                    loop
                RIUpdate event sink -> do
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
                    loop
    loop
    -- signal that we're done
    void $ takeMVar masterRepNThreadId

-- | Create a checkpoint (on all nodes, per request).
--   This is useful for faster resume of both the Master (at startup) and
--   Slaves (at startup and reconnect).
createMasterCheckpoint :: MasterState st -> IO ()
createMasterCheckpoint masterState@MasterState{..} = do
    debug "Checkpoint."
    unlocked <- isEmptyMVar masterStateLock
    unless unlocked $ error "State is locked."
    queueRepItem masterState RICheckpoint

-- | Create an archive on all nodes.
--   Usually createArchive (local to each node) is appropriate.
--   Also take care: Nodes that are not connected at the time, will not create
--   an archive (on reconnect).
createArchiveGlobally :: (IsAcidic st, Typeable st) => AcidState st -> IO ()
createArchiveGlobally acid = do
    debug "Archive globally."
    let masterState = downcast acid
    queueRepItem masterState RIArchive


toAcidState :: IsAcidic st => MasterState st -> AcidState st
toAcidState master
  = AcidState { _scheduleUpdate    = scheduleMasterUpdate master
              , scheduleColdUpdate = scheduleColdUpdate $ localState master
              , _query             = query $ localState master
              , queryCold          = queryCold $ localState master
              , createCheckpoint   = createMasterCheckpoint master
              , createArchive      = createArchive $ localState master
              , closeAcidState     = closeMasterState master
              , acidSubState       = mkAnyState master
              }

