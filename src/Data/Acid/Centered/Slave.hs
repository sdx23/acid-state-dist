{-# LANGUAGE DeriveDataTypeable, RecordWildCards, FlexibleContexts #-}
--------------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.CenteredSlave.hs
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  non-portable (uses GHC extensions)

  The Slave part of a the Centered replication backend for acid state.

-}

--------------------------------------------------------------------------------
-- SLAVE part

module Data.Acid.Centered.Slave
    (
      enslaveState
    , enslaveStateFrom
    , enslaveRedState
    , enslaveRedStateFrom
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

import Control.Concurrent (forkIO, ThreadId, myThreadId, killThread, threadDelay, forkIOWithUnmask)
import Control.Concurrent.MVar (MVar, newMVar, newEmptyMVar, isEmptyMVar,
                                withMVar, modifyMVar, modifyMVar_,
                                takeMVar, putMVar, tryPutMVar)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent.STM.TVar (readTVar, writeTVar)
import Data.IORef (writeIORef)
import qualified Control.Concurrent.Event as Event

import Control.Monad.STM (atomically)
import Control.Monad (void, when, unless)
import Control.Exception (handle, throwTo, SomeException, ErrorCall(..))

import System.ZMQ4 (Context, Socket, Dealer(..),
                    setReceiveHighWM, setSendHighWM, setLinger, restrict,
                    poll, Poll(..), Event(..),
                    context, term, socket, close,
                    connect, disconnect, send, receive)
import System.FilePath ( (</>) )

import Data.ByteString.Lazy.Char8 (ByteString)

import           Data.IntMap (IntMap)
import qualified Data.IntMap as IM

--------------------------------------------------------------------------------

-- | Slave state structure, for internal use.
data SlaveState st
    = SlaveState { slaveLocalState :: AcidState st
                 , slaveStateIsRed :: Bool
                 , slaveStateLock :: MVar ()
                 , slaveRepFinalizers :: MVar (IntMap (IO ()))
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
type SlaveRequests = IntMap (IO (IO ()),ThreadId)

-- | One Update + Metainformation to replicate.
data SlaveRepItem =
      SRIEnd
    | SRICheckpoint Revision
    | SRIArchive Revision
    | SRIUpdate Revision (Maybe RequestID) (Tagged ByteString)

-- | Open a local State as Slave for a Master.
--
-- The directory for the local state files is the default one ("state/[typeOf
-- state]").
enslaveState :: (IsAcidic st, Typeable st) =>
            String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveState address port initialState =
    enslaveStateFrom ("state" </> show (typeOf initialState)) address port initialState

-- | Open a local State as Slave for a Master.
--
-- The directory for the local state files is the default one ("state/[typeOf
-- state]").
enslaveRedState :: (IsAcidic st, Typeable st) =>
            String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveRedState address port initialState =
    enslaveRedStateFrom ("state" </> show (typeOf initialState)) address port initialState

-- | Open a local State as Slave for a Master. The directory of the local state
-- files can be specified.
enslaveStateFrom :: (IsAcidic st, Typeable st) =>
            FilePath        -- ^ location of the local state files.
         -> String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveStateFrom = enslaveMayRedStateFrom False

-- | Open a local State as Slave for a _redundant_ Master. The directory of the local state
-- files can be specified.
enslaveRedStateFrom :: (IsAcidic st, Typeable st) =>
            FilePath        -- ^ location of the local state files.
         -> String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveRedStateFrom = enslaveMayRedStateFrom True

-- | Open a local State as Slave for a Master, redundant or not.
--   The directory of the local state files can be specified.
enslaveMayRedStateFrom :: (IsAcidic st, Typeable st) =>
            Bool            -- ^ is redundant
         -> FilePath        -- ^ location of the local state files.
         -> String          -- ^ hostname of the Master
         -> PortNumber      -- ^ port to connect to
         -> st              -- ^ initial state
         -> IO (AcidState st)
enslaveMayRedStateFrom isRed directory address port initialState = do
        -- local
        lst <- openLocalStateFrom directory initialState
        lrev <- getLocalRevision lst
        rev <- newMVar lrev
        debug $ "Opening enslaved state at revision " ++ show lrev
        srs <- newMVar IM.empty
        lastReqId <- newMVar 0
        repChan <- newChan
        syncDone <- Event.new
        reqTid <- newEmptyMVar
        repTid <- newEmptyMVar
        parTid <- myThreadId
        repFin <- newMVar IM.empty
        sLock <- newEmptyMVar
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
                                    , slaveStateIsRed = isRed
                                    , slaveStateLock = sLock
                                    , slaveRepFinalizers = repFin
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
        void $ forkIOWithUnmask $ slaveRequestHandler slaveState
        void $ forkIO $ slaveReplicationHandler slaveState
        return $ slaveToAcidState slaveState
        where
            getLocalRevision =
                atomically . readTVar . logNextEntryId . localEvents . downcast

-- | Replication handler of the Slave.
slaveRequestHandler :: (IsAcidic st, Typeable st) => SlaveState st -> (IO () -> IO ()) -> IO ()
slaveRequestHandler slaveState@SlaveState{..} unmask = do
    mtid <- myThreadId
    putMVar slaveReqThreadId mtid
    let loop = handle (\e -> throwTo slaveParentThreadId (e :: SomeException)) $
            unmask $ handle killHandler $ do
            --waitRead =<< readMVar slaveZmqSocket
            -- FIXME: we needn't poll if not for strange zmq behaviour
            re <- withMVar slaveZmqSocket $ \sock -> poll 100 [Sock sock [In] Nothing]
            unless (null $ head re) $ do
                msg <- withMVar slaveZmqSocket receive
                case decode msg of
                    Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show str
                    Right mmsg -> handleMessage mmsg
            loop
    loop
    where
        killHandler :: AcidException -> IO ()
        killHandler GracefulExit = return ()
        handleMessage m = do
            debug $ "Received: " ++ show m
            case m of
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
               -- Full replication of a revision
               FullRep r -> modifyMVar_ slaveRepFinalizers $ \rf -> do
                               rf IM.! r
                               return $ IM.delete r rf
               -- Full replication of events up to revision
               FullRepTo r -> modifyMVar_ slaveRepFinalizers $ \rf -> do
                               let (ef, nrf) = IM.partitionWithKey (\k _ -> k <= r) rf
                               sequence_ (IM.elems ef)
                               return nrf
               -- We are allowed to Quit.
               MayQuit -> writeChan slaveRepChan SRIEnd
               -- We are requested to Quit - shall be handled by
               -- 'bracket' usage by user.
               MasterQuit -> throwTo slaveParentThreadId $
                   ErrorCall "Data.Acid.Centered.Slave: Master quit."
               -- no other messages possible, enforced by type checker

-- | After sync check CRC
onSyncDone :: (IsAcidic st, Typeable st) => SlaveState st -> Crc -> IO ()
onSyncDone SlaveState{..} crc = do
    localCrc <- crcOfState slaveLocalState
    if crc /= localCrc then
        error "Data.Acid.Centered.Slave: CRC mismatch after sync."
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
slaveReplicationHandler :: Typeable st => SlaveState st -> IO ()
slaveReplicationHandler slaveState@SlaveState{..} = do
        mtid <- myThreadId
        putMVar slaveRepThreadId mtid

        -- todo: timeout is magic variable, make customizable
        noTimeout <- Event.waitTimeout slaveSyncDone $ 10*1000*1000
        unless noTimeout $ throwTo slaveParentThreadId $
            ErrorCall "Data.Acid.Centered.Slave: Took too long to sync. Timeout."

        let loop = handle (\e -> throwTo slaveParentThreadId (e :: SomeException)) $ do
                mayRepItem <- readChan slaveRepChan
                case mayRepItem of
                    SRIEnd          -> return ()
                    SRICheckpoint r -> repCheckpoint slaveState r               >> loop
                    SRIArchive r    -> repArchive slaveState r                  >> loop
                    SRIUpdate r i d -> replicateUpdate slaveState r i d False   >> loop
        loop

        -- signal that we're done
        void $ takeMVar slaveRepThreadId

-- | Replicate Sync-Checkpoints directly.
replicateSyncCp :: (IsAcidic st, Typeable st) =>
        SlaveState st -> Revision -> ByteString -> IO ()
replicateSyncCp SlaveState{..} rev encoded = do
    st <- decodeCheckpoint encoded
    let lst = downcast slaveLocalState
    let core = localCore lst
    modifyMVar_ slaveRevision $ \sr -> do
        when (sr > rev) $ error "Data.Acid.Centered.Slave: Revision mismatch for checkpoint: Slave is newer."
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
                Left msg  -> error $ "Data.Acid.Centered.Slave: Checkpoint could not be decoded: " ++ msg
                Right val -> return val

-- | Replicate Sync-Updates directly.
replicateSyncUpdate :: Typeable st => SlaveState st -> Revision -> Tagged ByteString -> IO ()
replicateSyncUpdate slaveState rev event = replicateUpdate slaveState rev Nothing event True

-- | Replicate an Update as requested by Master.
--   Updates that were requested by this Slave are run locally and the result
--   put into the MVar in SlaveRequests.
--   Other Updates are just replicated without using the result.
replicateUpdate :: Typeable st => SlaveState st -> Revision -> Maybe RequestID -> Tagged ByteString -> Bool -> IO ()
replicateUpdate SlaveState{..} rev reqId event syncing = do
        debug $ "Got an Update to replicate " ++ show rev
        modifyMVar_ slaveRevision $ \nr -> if rev - 1 == nr
            then do
                -- commit / run it locally
                case reqId of
                    Nothing -> replicateForeign
                    Just rid -> replicateOwn rid
                -- send reply: we're done
                unless syncing $ sendToMaster slaveZmqSocket $ RepDone rev
                return rev
            else do
                sendToMaster slaveZmqSocket RepError
                void $ error $
                    "Data.Acid.Centered.Slave: Replication failed at revision "
                        ++ show nr ++ " -> " ++ show rev
                return nr
        where
            replicateForeign =
                if slaveStateIsRed then do
                    act <- newEmptyMVar >>= scheduleLocalColdUpdate' (downcast slaveLocalState) event
                    modifyMVar_ slaveRepFinalizers $ return . IM.insert rev act
                else
                    void $ scheduleColdUpdate slaveLocalState event
            replicateOwn rid = do
                act <- modifyMVar slaveRequests $ \srs -> do
                    debug $ "This is the Update for Request " ++ show rid
                    let (icallback, timeoutId) = srs IM.! rid
                    callback <- icallback
                    killThread timeoutId
                    let nsrs = IM.delete rid srs
                    return (nsrs, callback)
                when slaveStateIsRed $
                    modifyMVar_ slaveRepFinalizers $ return . IM.insert rev act

repCheckpoint :: SlaveState st -> Revision -> IO ()
repCheckpoint SlaveState{..} rev = do
    debug $ "Got Checkpoint request at revision: " ++ show rev
    withMVar slaveRevision $ \_ ->
        -- create checkpoint
        createCheckpoint slaveLocalState

repArchive :: SlaveState st -> Revision -> IO ()
repArchive SlaveState{..} rev = do
    debug $ "Got Archive request at revision: " ++ show rev
    withMVar slaveRevision $ \_ ->
        createArchive slaveLocalState


-- | Update on slave site.
--      The steps are:
--      - Request Update from Master
--      - Master issues Update with same RequestID
--      - repHandler replicates and puts result in MVar
scheduleSlaveUpdate :: (UpdateEvent e, Typeable (EventState e)) => SlaveState (EventState e) -> e -> IO (MVar (EventResult e))
scheduleSlaveUpdate slaveState@SlaveState{..} event = do
    unlocked <- isEmptyMVar slaveStateLock
    if not unlocked then error "State is locked."
    else do
        debug "Update by Slave."
        result <- newEmptyMVar
        reqId <- getNextRequestId slaveState
        modifyMVar_ slaveRequests $ \srs -> do
            let encoded = runPutLazy (safePut event)
            sendToMaster slaveZmqSocket $ ReqUpdate reqId (methodTag event, encoded)
            timeoutID <- forkIO $ timeoutRequest slaveState reqId result
            let callback = if slaveStateIsRed
                    then scheduleLocalUpdate' (downcast slaveLocalState) event result
                    else do
                        hd <- scheduleUpdate slaveLocalState event
                        void $ forkIO $ putMVar result =<< takeMVar hd
                        return (return ())      -- bogus finalizer
            return $ IM.insert reqId (callback, timeoutID) srs
        return result

-- | Cold Update on slave site. This enables for using Remote.
scheduleSlaveColdUpdate :: Typeable st => SlaveState st -> Tagged ByteString -> IO (MVar ByteString)
scheduleSlaveColdUpdate slaveState@SlaveState{..} encoded = do
    unlocked <- isEmptyMVar slaveStateLock
    if not unlocked then error "State is locked."
    else do
        debug "Cold Update by Slave."
        result <- newEmptyMVar
        -- slaveLastRequestID is only modified here - and used for locking the state
        reqId <- getNextRequestId slaveState
        modifyMVar_ slaveRequests $ \srs -> do
            sendToMaster slaveZmqSocket $ ReqUpdate reqId encoded
            timeoutID <- forkIO $ timeoutRequest slaveState reqId result
            let callback = if slaveStateIsRed
                    then scheduleLocalColdUpdate' (downcast slaveLocalState) encoded result
                    else do
                        hd <- scheduleColdUpdate slaveLocalState encoded
                        void $ forkIO $ putMVar result =<< takeMVar hd
                        return (return ())      -- bogus finalizer
            return $ IM.insert reqId (callback, timeoutID) srs
        return result

-- | Generate ID for another request.
getNextRequestId :: SlaveState st -> IO RequestID
getNextRequestId SlaveState{..} = modifyMVar slaveLastRequestID $ \x -> return (x+1,x+1)

-- | Ensures requests are actually answered or fail.
--   On timeout the Slave dies, not the thread that invoked the Update.
timeoutRequest :: SlaveState st -> RequestID -> MVar m -> IO ()
timeoutRequest SlaveState{..} reqId mvar = do
    threadDelay $ 5*1000*1000
    stillThere <- withMVar slaveRequests (return . IM.member reqId)
    when stillThere $ do
        putMVar mvar $ error "Data.Acid.Centered.Slave: Update-Request timed out."
        throwTo slaveParentThreadId $ ErrorCall "Data.Acid.Centered.Slave: Update-Request timed out."

-- | Send a message to Master.
sendToMaster :: MVar (Socket Dealer) -> SlaveMessage -> IO ()
sendToMaster msock smsg = withMVar msock $ \sock -> send sock [] (encode smsg)

-- | Close an enslaved State.
liberateState :: SlaveState st -> IO ()
liberateState SlaveState{..} =
    -- lock state against updates: disallow requests
    whenM (tryPutMVar slaveStateLock ()) $ do
        debug "Closing Slave state..."
        -- check / wait unprocessed requests
        debug "Waiting for Requests to finish."
        waitPoll 100 (withMVar slaveRequests (return . IM.null))
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
        withMVar slaveReqThreadId $ flip throwTo GracefulExit
        -- cleanup zmq
        debug "Closing down zmq."
        withMVar slaveZmqSocket $ \s -> do
            -- avoid the socket hanging around
            setLinger (restrict (1000 :: Int)) s
            disconnect s slaveZmqAddr
            close s
        term slaveZmqContext
        -- cleanup local state
        debug "Closing local state."
        closeAcidState slaveLocalState

slaveToAcidState :: (IsAcidic st, Typeable st)  => SlaveState st -> AcidState st
slaveToAcidState slaveState
  = AcidState { _scheduleUpdate    = scheduleSlaveUpdate slaveState
              , scheduleColdUpdate = scheduleSlaveColdUpdate slaveState
              , _query             = query $ slaveLocalState slaveState
              , queryCold          = queryCold $ slaveLocalState slaveState
              , createCheckpoint   = createCheckpoint $ slaveLocalState slaveState
              , createArchive      = createArchive $ slaveLocalState slaveState
              , closeAcidState     = liberateState slaveState
              , acidSubState       = mkAnyState slaveState
              }
