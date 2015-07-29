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

module Data.Acid.Centered.Slave
    (
      enslaveState
    , enslaveStateFrom
    , SlaveState(..)
    )  where

import Data.Typeable
import Data.SafeCopy
import Data.Serialize (decode, encode, runPutLazy, runGetLazy)

import Data.Acid
import Data.Acid.Core
import Data.Acid.Abstract
import Data.Acid.Local
import Data.Acid.Log

import Data.Acid.Centered.Common

import System.ZMQ4 (Context, Socket, Dealer(..),
                    setReceiveHighWM, setSendHighWM, restrict,
                    poll, Poll(..), Event(..),
                    context, term, socket, close,
                    connect, disconnect, send, receive)
import System.FilePath ( (</>) )

import Control.Concurrent (forkIO, throwTo, ThreadId, myThreadId, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newMVar, newEmptyMVar,
                                withMVar, modifyMVar, modifyMVar_,
                                takeMVar, putMVar)
import Data.IORef (writeIORef)
import Control.Monad (void, when, unless)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar, writeTVar)
import qualified Control.Concurrent.Event as Event
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Exception (handle, throw, SomeException, ErrorCall(..), AsyncException(..))

import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe (fromMaybe)

import qualified Data.ByteString.Lazy.Char8 as CSL

--------------------------------------------------------------------------------

-- | Slave state structure, for internal use.
data SlaveState st
    = SlaveState { slaveLocalState :: AcidState st
                 , slaveRepChan :: Chan SlaveRepItem
                 , slaveSyncDone :: Event.Event
                 , slaveRevision :: MVar NodeRevision
                 , slaveRequests :: MVar SlaveRequests
                 , slaveLastRequestID :: MVar RequestID
                 , slaveRepThreadId :: MVar ThreadId
                 , slaveReqThreadId :: MVar ThreadId
                 , slaveParentThreadId :: ThreadId
                 , slaveZmqContext :: Context
                 , slaveZmqAddr :: String
                 , slaveZmqSocket :: MVar (Socket Dealer)
                 } deriving (Typeable)

-- | Memory of own Requests sent to Master.
type SlaveRequests = Map RequestID (IO (),ThreadId)

-- | One Update + Metainformation to replicate.
data SlaveRepItem =
      SRIEnd
    | SRICheckpoint Revision
    | SRIArchive Revision
    | SRIUpdate Revision (Maybe RequestID) (Tagged CSL.ByteString)

-- | Open a local State as Slave for a Master. The directory for the local state
-- files is the default one ("state/NameOfState").
enslaveState :: (IsAcidic st, Typeable st) =>
            String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveState address port initialState =
    enslaveStateFrom ("state" </> show (typeOf initialState)) address port initialState

-- | Open a local State as Slave for a Master. The directory of the local state
-- files can be specified.
enslaveStateFrom :: (IsAcidic st, Typeable st) =>
            FilePath        -- ^ location of the local state files.
         -> String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveStateFrom directory address port initialState = do
        -- local
        lst <- openLocalStateFrom directory initialState
        let levs = localEvents $ downcast lst
        lrev <- atomically $ readTVar $ logNextEntryId levs
        rev <- newMVar lrev
        debug $ "Opening enslaved state at revision " ++ show lrev
        srs <- newMVar M.empty
        lastReqId <- newMVar 0
        repChan <- newChan
        syncDone <- Event.new
        reqTid <- newEmptyMVar
        repTid <- newEmptyMVar
        parTid <- myThreadId
        -- remote
        let addr = "tcp://" ++ address ++ ":" ++ show port
        ctx <- context
        sock <- socket ctx Dealer
        setReceiveHighWM (restrict (100*1000 :: Int)) sock
        setSendHighWM (restrict (100*1000 :: Int)) sock
        connect sock addr
        msock <- newMVar sock
        sendToMaster msock $ NewSlave lrev
        let slaveState = SlaveState { slaveLocalState = lst
                                    , slaveRepChan = repChan
                                    , slaveSyncDone = syncDone
                                    , slaveRevision = rev
                                    , slaveRequests = srs
                                    , slaveLastRequestID = lastReqId
                                    , slaveReqThreadId = reqTid
                                    , slaveRepThreadId = repTid
                                    , slaveParentThreadId = parTid
                                    , slaveZmqContext = ctx
                                    , slaveZmqAddr = addr
                                    , slaveZmqSocket = msock
                                    }
        void $ forkIO $ slaveRequestHandler slaveState
        void $ forkIO $ slaveReplicationHandler slaveState
        return $ slaveToAcidState slaveState

-- | Replication handler of the Slave.
slaveRequestHandler :: (IsAcidic st, Typeable st) => SlaveState st -> IO ()
slaveRequestHandler slaveState@SlaveState{..} = do
    mtid <- myThreadId
    putMVar slaveReqThreadId mtid
    let loop = handle (\e -> throwTo slaveParentThreadId (e :: SomeException)) $
          handle killHandler $ do
            --waitRead =<< readMVar slaveZmqSocket
            -- FIXME: we needn't poll if not for strange zmq behaviour
            re <- withMVar slaveZmqSocket $ \sock -> poll 100 [Sock sock [In] Nothing]
            unless (null $ head re) $ do
                msg <- withMVar slaveZmqSocket receive
                case decode msg of
                    Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show str
                    Right mmsg -> do
                         debug $ "Received: " ++ show mmsg
                         case mmsg of
                            -- We are sent an Update to replicate.
                            DoRep r i d -> queueRepItem slaveState (SRIUpdate r i d)
                            -- We are sent a Checkpoint for synchronization.
                            DoSyncCheckpoint r d -> replicateSyncCp slaveState r d
                            -- We are sent an Update to replicate for synchronization.
                            DoSyncRep r d -> replicateSyncUpdate slaveState r d
                            -- Master done sending all synchronization Updates.
                            SyncDone c -> onSyncDone slaveState c
                            -- We are sent a Checkpoint request.
                            DoCheckpoint r -> queueRepItem slaveState (SRICheckpoint r)
                            -- We are sent an Archive request.
                            DoArchive r -> queueRepItem slaveState (SRIArchive r)
                            -- We are allowed to Quit.
                            MayQuit -> writeChan slaveRepChan SRIEnd
                            -- We are requested to Quit.
                            MasterQuit -> void $ forkIO $ liberateState slaveState
                            -- no other messages possible, enforced by type checker
            loop
    loop
    where
        -- FIXME: actually we'd like our own exception for graceful exit
        killHandler :: AsyncException -> IO ()
        killHandler ThreadKilled = return ()
        killHandler e = throw e

-- | After sync check CRC
onSyncDone :: (IsAcidic st, Typeable st) => SlaveState st -> Crc -> IO ()
onSyncDone slaveState@SlaveState{..} crc = do
    localCrc <- crcOfState slaveLocalState
    if crc /= localCrc then do
        -- TODO: this is an error
        putStrLn "Data.Acid.Centered.Slave: CRC mismatch after sync. Exiting."
        void $ forkIO $ liberateState slaveState
    else do
        debug "Sync Done, CRC fine."
        Event.set slaveSyncDone

-- | Queue Updates into Chan for replication.
-- We use the Chan so Sync-Updates and normal ones can be interleaved.
queueRepItem :: SlaveState st -> SlaveRepItem -> IO ()
queueRepItem SlaveState{..} repItem = do
        debug "Queuing RepItem."
        writeChan slaveRepChan repItem

-- | Replicates content of Chan.
slaveReplicationHandler :: SlaveState st -> IO ()
slaveReplicationHandler slaveState@SlaveState{..} = do
        mtid <- myThreadId
        putMVar slaveRepThreadId mtid
        -- todo: timeout is magic variable, make customizable?
        noTimeout <- Event.waitTimeout slaveSyncDone $ 10*1000*1000
        unless noTimeout $ throwTo slaveParentThreadId $ ErrorCall "Slave took too long to sync, ran into timeout."
        let loop = handle (\e -> throwTo slaveParentThreadId (e :: SomeException)) $ do
                mayRepItem <- readChan slaveRepChan
                case mayRepItem of
                    SRIEnd -> return ()
                    SRICheckpoint r -> do
                        repCheckpoint slaveState r
                        loop
                    SRIArchive r -> do
                        repArchive slaveState r
                        loop
                    SRIUpdate r i d -> do
                        replicateUpdate slaveState r i d False
                        loop
        loop
        -- signal that we're done
        void $ takeMVar slaveRepThreadId

-- | Replicate Sync-Checkpoints directly.
replicateSyncCp :: (IsAcidic st, Typeable st) =>
        SlaveState st -> Revision -> CSL.ByteString -> IO ()
replicateSyncCp SlaveState{..} rev encoded = do
    st <- decodeCheckpoint encoded
    let lst = downcast slaveLocalState
    let core = localCore lst
    modifyMVar_ slaveRevision $ \sr -> do
        when (sr > rev) $ error "Revision mismatch for checkpoint: Slave is newer."
        -- todo: check
        modifyCoreState_ core $ \_ -> do
            writeIORef (localCopy lst) st
            createCpFake lst encoded rev
            adjustEventLogId lst rev
            return st
        return rev
    where
        adjustEventLogId l r = do
            atomically $ writeTVar (logNextEntryId (localEvents l)) r
            void $ cutFileLog (localEvents l)
        createCpFake l e r = do
            mvar <- newEmptyMVar
            pushAction (localEvents l) $
                pushEntry (localCheckpoints l) (Checkpoint r e) (putMVar mvar ())
            takeMVar mvar
        decodeCheckpoint e =
            case runGetLazy safeGet e of
                Left msg  -> error $ "Checkpoint could not be decoded: " ++ msg
                Right val -> return val

-- | Replicate Sync-Updates directly.
replicateSyncUpdate :: SlaveState st -> Revision -> Tagged CSL.ByteString -> IO ()
replicateSyncUpdate slaveState rev event = replicateUpdate slaveState rev Nothing event True

-- | Replicate an Update as requested by Master.
--   Updates that were requested by this Slave are run locally and the result
--   put into the MVar in SlaveRequests.
--   Other Updates are just replicated without using the result.
replicateUpdate :: SlaveState st -> Revision -> Maybe RequestID -> Tagged CSL.ByteString -> Bool -> IO ()
replicateUpdate SlaveState{..} rev reqId event syncing = do
        debug $ "Got an Update to replicate " ++ show rev
        modifyMVar_ slaveRevision $ \nr -> if rev - 1 == nr
            then do
                -- commit / run it locally
                case reqId of
                    Nothing ->
                        void $ scheduleColdUpdate slaveLocalState event
                    Just rid -> modifyMVar slaveRequests $ \srs -> do
                        debug $ "This is the Update for Request " ++ show rid
                        let (icallback, timeoutId) = fromMaybe (error $ "Callback not found: " ++ show rid) (M.lookup rid srs)
                        callback <- icallback
                        killThread timeoutId
                        let nsrs = M.delete rid srs
                        return (nsrs, callback)
                -- send reply: we're done
                unless syncing $ sendToMaster slaveZmqSocket $ RepDone rev
                return rev
            else do
                sendToMaster slaveZmqSocket RepError
                void $ error $ "Replication failed at revision " ++ show rev ++ " -> " ++ show nr
                return nr

repCheckpoint :: SlaveState st -> Revision -> IO ()
repCheckpoint SlaveState{..} rev = do
    debug $ "Got Checkpoint request at revision: " ++ show rev
    -- todo: check that we're at the correct revision
    withMVar slaveRevision $ \_ ->
        -- create checkpoint
        createCheckpoint slaveLocalState

repArchive :: SlaveState st -> Revision -> IO ()
repArchive SlaveState{..} rev = do
    debug $ "Got Archive request at revision: " ++ show rev
    -- todo: at right revision?
    withMVar slaveRevision $ \_ ->
        createArchive slaveLocalState


-- | Update on slave site.
--      The steps are:
--      - Request Update from Master
--      - Master issues Update with same RequestID
--      - repHandler replicates and puts result in MVar
scheduleSlaveUpdate :: UpdateEvent e => SlaveState (EventState e) -> e -> IO (MVar (EventResult e))
scheduleSlaveUpdate slaveState@SlaveState{..} event = do
        debug "Update by Slave."
        result <- newEmptyMVar
        -- slaveLastRequestID is only modified here - and used for locking the state
        reqId <- modifyMVar slaveLastRequestID $ \x -> return (x+1,x+1)
        modifyMVar_ slaveRequests $ \srs -> do
            let encoded = runPutLazy (safePut event)
            sendToMaster slaveZmqSocket $ ReqUpdate reqId (methodTag event, encoded)
            timeoutID <- forkIO $ timeoutRequest slaveState reqId
            let callback = do
                    hd <- scheduleUpdate slaveLocalState event
                    void $ forkIO $ putMVar result =<< takeMVar hd
            return $ M.insert reqId (callback, timeoutID) srs
        return result

-- | Ensures requests are actually answered or fail.
-- TODO: put error into result-mvar too
timeoutRequest :: SlaveState st -> RequestID -> IO ()
timeoutRequest SlaveState{..} reqId = do
    threadDelay $ 5*1000*1000
    stillThere <- withMVar slaveRequests (return . M.member reqId)
    when stillThere $ throwTo slaveParentThreadId $ ErrorCall "Update-Request timed out."

-- | Send a message to Master.
sendToMaster :: MVar (Socket Dealer) -> SlaveMessage -> IO ()
sendToMaster msock smsg = withMVar msock $ \sock -> send sock [] (encode smsg)

-- | Close an enslaved State.
liberateState :: SlaveState st -> IO ()
liberateState SlaveState{..} = do
        debug "Closing Slave state..."
        -- lock state against updates: disallow requests
        -- todo: rather use a special value allowing exceptions in scheduleUpdate
        _ <- takeMVar slaveLastRequestID
        -- check / wait unprocessed requests
        debug "Waiting for Requests to finish."
        waitPoll 100 (withMVar slaveRequests (return . M.null))
        -- send master quit message
        sendToMaster slaveZmqSocket SlaveQuit
        -- wait replication chan, only if sync done
        syncDone <- Event.isSet slaveSyncDone
        when syncDone $ do
            debug "Waiting for repChan to empty."
            mtid <- myThreadId
            putMVar slaveRepThreadId mtid
        -- kill handler threads
        debug "Killing request handler."
        withMVar slaveReqThreadId killThread
        --withMVar slaveReqThreadId $ \t -> throwTo t GracefulExit
        -- cleanup zmq
        debug "Closing down zmq."
        withMVar slaveZmqSocket $ \s -> do
            disconnect s slaveZmqAddr
            close s
        term slaveZmqContext
        -- cleanup local state
        debug "Closing local state."
        closeAcidState slaveLocalState

slaveToAcidState :: IsAcidic st => SlaveState st -> AcidState st
slaveToAcidState slaveState
  = AcidState { _scheduleUpdate    = scheduleSlaveUpdate slaveState
              , scheduleColdUpdate = undefined
              , _query             = query $ slaveLocalState slaveState
              , queryCold          = queryCold $ slaveLocalState slaveState
              , createCheckpoint   = createCheckpoint $ slaveLocalState slaveState
              , createArchive      = createArchive $ slaveLocalState slaveState
              , closeAcidState     = liberateState slaveState
              , acidSubState       = mkAnyState slaveState
              }
