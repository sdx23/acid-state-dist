{-# LANGUAGE DeriveDataTypeable, RecordWildCards, FlexibleContexts #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.Centered.Master
  Copyright   :  MIT

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  non-portable (uses GHC extensions)

  The Master part of the Centered replication backend for acid state.

-}
{- big chunks still todo:
    o authentification
    o encryption
-}
module Data.Acid.Centered.Master
    (
      openMasterState
    , openMasterStateFrom
    , openRedMasterState
    , openRedMasterStateFrom
    , createArchiveGlobally
    , MasterState(..)
    ) where

import Data.Typeable
import Data.SafeCopy
import Data.Serialize (decode, encode, runPutLazy)

import Data.Acid
import Data.Acid.Core
import Data.Acid.Abstract
import Data.Acid.Local
import Data.Acid.Log

import Data.Acid.Centered.Common

import Control.Concurrent (forkIO, ThreadId, myThreadId)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan, dupChan)
import Control.Concurrent.STM.TVar (readTVar)
import Control.Concurrent.MVar(MVar, newMVar, newEmptyMVar,
                               takeMVar, putMVar, tryPutMVar, isEmptyMVar,
                               modifyMVar, modifyMVar_, withMVar)

import Control.Monad.STM (atomically)
import Control.Monad (when, unless, void, forM_, liftM2)
import Control.Exception (handle, throwTo, SomeException)

import System.ZMQ4 (Context, Socket, Router(..), Receiver,
                    setReceiveHighWM, setSendHighWM, restrict,
                    context, term, socket, close, bind, unbind,
                    poll, Poll(..), Event(..),
                    sendMulti, receiveMulti)
import System.FilePath ( (</>) )

import qualified Data.ByteString.Lazy.Char8 as CSL
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Char8 as CS

import qualified Data.Map as M
import           Data.Map (Map)
import qualified Data.IntMap as IM
import           Data.IntMap (IntMap)
import qualified Data.List.NonEmpty as NEL
import Safe (headDef)

--------------------------------------------------------------------------------

-- | Master state structure, for internal use.
data MasterState st
    = MasterState { localState :: AcidState st
                  , nodeStatus :: MVar NodeStatus
                  , repRedundancy :: Int
                  , repFinalizers :: MVar (IntMap (IO ()))
                  , masterStateLock :: MVar ()
                  , masterRevision :: MVar NodeRevision
                  , masterRevisionN :: MVar NodeRevision
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

type NodeIdentity = CS.ByteString
type NodeStatus = Map NodeIdentity NodeRevision
type Callback = IO (IO ())      -- an IO action that returns a finalizer
data ReplicationItem =
      RIEnd
    | RICheckpoint
    | RIArchive
    | RIUpdate (Tagged ByteString) (Either Callback (RequestID, NodeIdentity))

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
                handleMessage ident msg
            loop
    loop
    where
        killHandler :: AcidException -> IO ()
        killHandler GracefulExit = return ()
        identityIsValid i = do
            isMember <- withMVar nodeStatus $ return . (i `M.member`)
            if isMember then return True
            else do
                debug $ "Request by unknown node [" ++ CS.unpack i ++ "]"
                sendToSlave zmqSocket MayQuit i
                return False
        handleMessage i m = case m of
             -- New Slave joined.
             NewSlave r -> connectNode masterState i r
             -- Slave is done replicating.
             RepDone r -> whenM (identityIsValid i) $
                 updateNodeStatus masterState i r
             -- Slave sends an Udate.
             ReqUpdate rid event -> whenM (identityIsValid i) $
                 queueRepItem masterState (RIUpdate event (Right (rid, i)))
             -- Slave quits.
             SlaveQuit -> do
                 sendToSlave zmqSocket MayQuit i
                 removeFromNodeStatus nodeStatus i
             RepError -> do
                 sendToSlave zmqSocket MayQuit i
                 removeFromNodeStatus nodeStatus i
             -- no other messages possible

-- | Remove a Slave node from NodeStatus.
removeFromNodeStatus :: MVar NodeStatus -> NodeIdentity -> IO ()
removeFromNodeStatus nodeStatus ident =
    modifyMVar_ nodeStatus $ return . M.delete ident

-- | Update the NodeStatus after a node has replicated an Update.
updateNodeStatus :: MasterState st -> NodeIdentity -> Int -> IO ()
updateNodeStatus MasterState{..} ident rev =
    modifyMVar_ nodeStatus $ \ns -> do
        when (ns M.! ident /= (rev - 1)) $
            error $ "Invalid increment of node status "
                ++ show (ns M.! ident) ++ " -> " ++ show rev
        let rns = M.adjust (+1) ident ns
        -- only for redundant operation:
        when ((repRedundancy > 1) && (M.size (M.filter (>=rev) rns) >= (repRedundancy - 1))) $ do
            debug $ "Full replication of " ++ show rev
            -- finalize local replication
            runAndDelFinalizer rev
            -- send out FullRep signal
            forM_ (M.keys ns) $ sendToSlave zmqSocket (FullRep rev)
        return rns
    where
        runAndDelFinalizer r = modifyMVar_ repFinalizers $ \rf -> do
            rf IM.! r
            return $ IM.delete r rf

-- | Connect a new Slave by getting it up-to-date,
--   i.e. send all past events as Updates. This is fire&forget.
connectNode :: (IsAcidic st, Typeable st) => MasterState st -> NodeIdentity -> Revision -> IO ()
connectNode MasterState{..} i revision =
    -- locking masterRevision prohibits additional events written on disk
    withMVar masterRevision $ \mr ->
        modifyMVar_ nodeStatus $ \ns -> do
            -- crc generated from localCore thus corresponds to disk
            crc <- crcOfState localState
            -- if there has been a checkpoint in between:
            lastCp <- getLastCheckpointRev localState
            let lastCpRev = cpRevision lastCp
            debug $ "Found checkpoint at revision " ++ show lastCpRev
            if lastCpRev > revision then do
                -- send last checkpoint and newer events
                sendSyncCheckpoint zmqSocket lastCp i
                pastUpdates <- getPastUpdates localState lastCpRev
                forM_ pastUpdates $ \(r, u) -> sendSyncUpdate zmqSocket r u i
            else do
                -- just the events
                pastUpdates <- getPastUpdates localState revision
                forM_ pastUpdates $ \(r, u) -> sendSyncUpdate zmqSocket r u i
            -- now done, crc
            sendToSlave zmqSocket (SyncDone crc) i
            let nns = M.insert i mr ns
            -- only for redundant operation:
            when (repRedundancy > 1) $ checkRepStatus mr nns
            return nns
    where
        cpRevision (Checkpoint r _) = r
        sendSyncCheckpoint sock (Checkpoint cr encoded) =
            sendToSlave sock (DoSyncCheckpoint cr encoded)
        sendSyncUpdate sock r encoded =
            sendToSlave sock (DoSyncRep r encoded)
        -- FIXME: do this better (less than maxRev is possible in corner cases)
        checkRepStatus maxRev pns =
            when (M.size (M.filter (>= maxRev) pns) >= (repRedundancy-2)) $ do
                debug $ "Full replication up to " ++ show maxRev
                -- finalize local replication
                modifyMVar_ repFinalizers $ \rf -> do
                    forM_ (filter (<= maxRev) (IM.keys rf)) $ \r -> rf IM.! r
                    return $ IM.filterWithKey (\k _ -> k > maxRev) rf
                -- send out FullRep signal
                forM_ (M.keys pns) $ sendToSlave zmqSocket (FullRepTo maxRev)


-- | Fetch past Updates from FileLog for replication.
getPastUpdates :: (Typeable st) => AcidState st -> Int -> IO [(Int, Tagged ByteString)]
getPastUpdates state startRev =
    liftM2 zip (return [(startRev+1)..]) (readEntriesFrom (localEvents $ downcast state) startRev)

-- | Get the revision at which the last checkpoint was taken.
getLastCheckpointRev :: (Typeable st) => AcidState st -> IO Checkpoint
getLastCheckpointRev state = do
    let cplog = localCheckpoints $ downcast state
    nextId <- atomically $ readTVar $ logNextEntryId cplog
    cps <- readEntriesFrom cplog (nextId - 1)
    return $ headDef (Checkpoint 0 CSL.empty) cps

-- | Send a message to a Slave
sendToSlave :: MVar (Socket Router) -> MasterMessage -> NodeIdentity -> IO ()
sendToSlave msock msg ident = withMVar msock $ \sock -> sendMulti sock $ NEL.fromList [ident, encode msg]

-- | Receive one Frame. A Frame consists of two messages:
--      sender ID and actual content
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

-- | Open the Master state.
--
-- The directory for the local state files is the default one ("state/[typeOf state]/").
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
openMasterStateFrom directory address port =
    openRedMasterStateFrom directory address port 0

-- | Open the master state with /n/-redundant replication.
--
-- The directory for the local state files is the default one ("state/[typeOf
-- state]/").
openRedMasterState :: (IsAcidic st, Typeable st) =>
               String       -- ^ address to bind (useful to listen on specific interfaces only)
            -> PortNumber   -- ^ port to bind to
            -> Int          -- ^ guarantee n-redundant replication
            -> st           -- ^ initial state
            -> IO (AcidState st)
openRedMasterState address port red initialState =
    openRedMasterStateFrom ("state" </> show (typeOf initialState)) address port red initialState

-- | Open the master state from a specific location with redundant replication.
openRedMasterStateFrom :: (IsAcidic st, Typeable st) =>
               FilePath     -- ^ location of the local state files
            -> String       -- ^ address to bind (useful to listen on specific interfaces only)
            -> PortNumber   -- ^ port to bind to
            -> Int          -- ^ guarantee /n/-redundant replication
            -> st           -- ^ initial state
            -> IO (AcidState st)
openRedMasterStateFrom directory address port red initialState = do
    debug "opening master state"
    -- local
    lst <- openLocalStateFrom directory initialState
    let levs = localEvents $ downcast lst
    lrev <- atomically $ readTVar $ logNextEntryId levs
    rev <- newMVar lrev
    revN <- newMVar lrev
    repChan <- newChan
    repChanN <- dupChan repChan
    repFin <- newMVar IM.empty
    ns <- newMVar M.empty
    repTidL <- newEmptyMVar
    repTidN <- newEmptyMVar
    reqTid <- newEmptyMVar
    parTid <- myThreadId
    sLock <- newEmptyMVar
    -- remote
    let addr = "tcp://" ++ address ++ ":" ++ show port
    ctx <- context
    sock <- socket ctx Router
    setReceiveHighWM (restrict (100*1000 :: Int)) sock
    setSendHighWM (restrict (100*1000 :: Int)) sock
    bind sock addr
    msock <- newMVar sock

    let masterState = MasterState { localState = lst
                                  , nodeStatus = ns
                                  , repRedundancy = red
                                  , repFinalizers = repFin
                                  , masterStateLock = sLock
                                  , masterRevision = rev
                                  , masterRevisionN = revN
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
closeMasterState MasterState{..} =
    -- disallow requests
    whenM (tryPutMVar masterStateLock ()) $ do
        debug "Closing master state."
        -- send nodes quit
        debug "Nodes quitting."
        withMVar nodeStatus $ mapM_ (sendToSlave zmqSocket MasterQuit) . M.keys
        -- wait all nodes done
        waitPollN 100 1000 (withMVar nodeStatus (return . M.null))
        -- wait replication chan
        debug "Waiting for repChans to empty."
        writeChan masterReplicationChan RIEnd
        mtid <- myThreadId
        putMVar masterRepLThreadId mtid
        putMVar masterRepNThreadId mtid
        -- kill handler
        debug "Killing request handler."
        withMVar masterReqThreadId $ flip throwTo GracefulExit
        -- cleanup zmq
        debug "Closing down zmq."
        withMVar zmqSocket $ \sock -> do
            unbind sock zmqAddr
            close sock
        term zmqContext
        -- cleanup local state
        closeAcidState localState

-- | Update on master site.
scheduleMasterUpdate :: (UpdateEvent event, Typeable (EventState event)) => MasterState (EventState event) -> event -> IO (MVar (EventResult event))
scheduleMasterUpdate masterState@MasterState{..} event = do
    debug "Update by Master."
    unlocked <- isEmptyMVar masterStateLock
    if not unlocked then error "State is locked!"
    else do
        result <- newEmptyMVar
        let callback = if repRedundancy > 1
            then
                -- the returned action fills in result when executed later
                scheduleLocalUpdate' (downcast localState) event result
            else do
                hd <- scheduleUpdate localState event
                void $ forkIO (putMVar result =<< takeMVar hd)
                return (return ())      -- bogus finalizer
        let encoded = runPutLazy (safePut event)
        queueRepItem masterState (RIUpdate (methodTag event, encoded) (Left callback))
        return result

-- | Cold Update on master site.
scheduleMasterColdUpdate :: Typeable st => MasterState st -> Tagged ByteString -> IO (MVar ByteString)
scheduleMasterColdUpdate masterState@MasterState{..} encoded = do
    debug "Cold Update by Master."
    unlocked <- isEmptyMVar masterStateLock
    if not unlocked then error "State is locked!"
    else do
        result <- newEmptyMVar
        let callback = if repRedundancy > 1
            then
                -- the returned action fills in result when executed later
                scheduleLocalColdUpdate' (downcast localState) encoded result
            else do
                hd <- scheduleColdUpdate localState encoded
                void $ forkIO (putMVar result =<< takeMVar hd)
                return (return ())      -- bogus finalizer
        queueRepItem masterState (RIUpdate encoded (Left callback))
        return result

-- | Queue an RepItem (originating from the Master itself of an Slave via zmq)
queueRepItem :: MasterState st -> ReplicationItem -> IO ()
queueRepItem MasterState{..} = writeChan masterReplicationChan

-- | The local replication handler. Takes care to run Updates locally.
masterReplicationHandlerL :: (Typeable st) => MasterState st -> IO ()
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
                    if repRedundancy > 1 then do
                        (rev, act) <- modifyMVar masterRevision $ \r -> do
                            a <- case sink of
                                Left callback   -> callback
                                _               -> newEmptyMVar >>= scheduleLocalColdUpdate' (downcast localState) event
                            return (r+1,(r+1,a))
                        -- act finalizes the transaction - will be run after full replication
                        modifyMVar_ repFinalizers $ return . IM.insert rev act
                    else
                        modifyMVar_ masterRevision $ \r -> do
                            case sink of
                                Left callback   -> void callback
                                _               -> void $ scheduleColdUpdate localState event
                            return (r+1)
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
                        withMVar masterRevisionN $ \mr ->
                            forM_ (M.keys ns) $ sendArchive zmqSocket mr
                    loop
                RICheckpoint -> do
                    withMVar nodeStatus $ \ns -> do
                        debug "Sending Checkpoint Request to Slaves."
                        withMVar masterRevisionN $ \mr ->
                            forM_ (M.keys ns) $ sendCheckpoint zmqSocket mr
                    loop
                RIUpdate event sink -> do
                    withMVar nodeStatus $ \ns -> do
                        debug $ "Sending Update to Slaves, there are " ++ show (M.size ns)
                        modifyMVar_ masterRevisionN $ \mrOld -> do
                            let mr = mrOld + 1
                            case sink of
                                Left _ -> forM_ (M.keys $ M.filter (<mr) ns) $ sendUpdate zmqSocket mr Nothing event
                                Right (reqID, reqNodeIdent) -> do
                                    let noReqSlaves = filter (/= reqNodeIdent) $ M.keys $ M.filter (<mr) ns
                                    sendUpdate zmqSocket mr (Just reqID) event reqNodeIdent
                                    forM_ noReqSlaves $ sendUpdate zmqSocket mr Nothing event
                            return mr
                    loop
    loop
    -- signal that we're done
    void $ takeMVar masterRepNThreadId
    where
        sendUpdate sock revision reqId encoded =
            sendToSlave sock (DoRep revision reqId encoded)
        sendCheckpoint sock revision = sendToSlave sock (DoCheckpoint revision)
        sendArchive sock revision = sendToSlave sock (DoArchive revision)

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


toAcidState :: (IsAcidic st, Typeable st) => MasterState st -> AcidState st
toAcidState master
  = AcidState { _scheduleUpdate    = scheduleMasterUpdate master
              , scheduleColdUpdate = scheduleMasterColdUpdate master
              , _query             = query $ localState master
              , queryCold          = queryCold $ localState master
              , createCheckpoint   = createMasterCheckpoint master
              , createArchive      = createArchive $ localState master
              , closeAcidState     = closeMasterState master
              , acidSubState       = mkAnyState master
              }

