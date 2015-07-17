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
--      request to be updated -> sync happens
--      whilst syncing normal updates accumulate in RepChan
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
    )  where

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

import System.ZMQ4 (Context, Socket, Dealer(..), Receiver, Flag(..),
                    setReceiveHighWM, setSendHighWM, restrict,
                    waitRead,
                    poll, Poll(..), Event(..),
                    context, term, socket, close, 
                    connect, disconnect,
                    send, receive)

import Control.Concurrent (forkIO, threadDelay, ThreadId, myThreadId, killThread)
import Control.Concurrent.MVar (MVar, newMVar, newEmptyMVar, 
                                withMVar, modifyMVar, modifyMVar_,
                                readMVar,
                                takeMVar, putMVar)
import Control.Monad (forever, void,
                      when, unless,
                      forM_
                     )
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)
import qualified Control.Concurrent.Event as Event
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)

import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe (fromMaybe)

import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as CS
import qualified Data.ByteString.Lazy.Char8 as CSL

--------------------------------------------------------------------------------

data SlaveState st 
    = SlaveState { slaveLocalState :: AcidState st
                 , slaveRepChan :: Chan SlaveRepItem
                 , slaveSyncDone :: Event.Event
                 , slaveRevision :: MVar NodeRevision
                 , slaveRequests :: MVar SlaveRequests
                 , slaveLastRequestID :: MVar RequestID
                 , slaveRepThreadId :: MVar ThreadId
                 , slaveReqThreadId :: MVar ThreadId
                 , slaveZmqContext :: Context
                 , slaveZmqAddr :: String
                 , slaveZmqSocket :: MVar (Socket Dealer)
                 } deriving (Typeable)

-- | Memory of own Requests sent to Master.
type SlaveRequests = Map RequestID (IO ())

-- | One Update + Metainformation to replicate.
data SlaveRepItem =
      SRIEnd
    | SRICheckpoint Revision
    | SRIUpdate Revision (Maybe RequestID) (Tagged CSL.ByteString)

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
        debug $ "Opening enslaved state at revision " ++ show lrev
        srs <- newMVar M.empty
        lastReqId <- newMVar 0
        repChan <- newChan
        syncDone <- Event.new
        sockLock <- newMVar ()
        reqTid <- newEmptyMVar
        repTid <- newEmptyMVar
        -- remote
        let addr = "tcp://" ++ address ++ ":" ++ show port
        ctx <- context
        sock <- socket ctx Dealer
        setReceiveHighWM (restrict (100*1000)) sock
        setSendHighWM (restrict (100*1000)) sock
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
                                    , slaveZmqContext = ctx
                                    , slaveZmqAddr = addr
                                    , slaveZmqSocket = msock
                                    }
        forkIO $ slaveRequestHandler slaveState 
        forkIO $ slaveReplicationHandler slaveState 
        return $ slaveToAcidState slaveState 

-- | Replication handler of the Slave. 
slaveRequestHandler :: (IsAcidic st, Typeable st) => SlaveState st -> IO ()
slaveRequestHandler slaveState@SlaveState{..} = do
    mtid <- myThreadId
    putMVar slaveReqThreadId mtid
    forever $ do
        --waitRead =<< readMVar slaveZmqSocket
        -- FIXME: we needn't poll if not for strange zmq behaviour
        re <- withMVar slaveZmqSocket $ \sock -> poll 100 [Sock sock [In] Nothing]
        unless (null $ head re) $ do
            msg <- withMVar slaveZmqSocket receive
            case decode msg of
                Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show msg
                Right mmsg -> do
                     debug $ "Received: " ++ show mmsg
                     case mmsg of
                        -- We are sent an Update to replicate.
                        DoRep r i d -> queueRepItem slaveState (SRIUpdate r i d)
                        -- We are sent an Update to replicate for synchronization.
                        DoSyncRep r d -> replicateSyncUpdate slaveState r d 
                        -- Master done sending all synchronization Updates.
                        SyncDone c -> onSyncDone slaveState c
                        -- We are sent an Checkpoint request.
                        DoCheckpoint r -> queueRepItem slaveState (SRICheckpoint r) 
                        -- We are allowed to Quit.
                        MayQuit -> writeChan slaveRepChan SRIEnd
                        -- We are requested to Quit.
                        MasterQuit -> void $ forkIO $ liberateState slaveState
                        -- no other messages possible
                        _ -> error $ "Unknown message received: " ++ show mmsg

-- | After sync check CRC
onSyncDone :: (IsAcidic st, Typeable st) => SlaveState st -> Crc -> IO ()
onSyncDone slaveState@SlaveState{..} crc = do
    localCrc <- crcOfState slaveLocalState
    if crc /= localCrc then do
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
        unless noTimeout $ error "Slave took too long to sync, ran into timeout."
        let loop = do
                mayRepItem <- readChan slaveRepChan
                case mayRepItem of
                    SRIEnd -> return ()
                    SRICheckpoint r -> do
                        repCheckpoint slaveState r
                        loop
                    SRIUpdate r i d -> do
                        replicateUpdate slaveState r i d False
                        loop
        loop
        -- signal that we're done
        void $ takeMVar slaveRepThreadId

-- | Replicate Sync-Updates directly.
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
                        callback <- fromMaybe (error $ "Callback not found: " ++ show rid) (M.lookup rid srs)
                        -- todo: we remember it, clean it up later
                        let nsrs = M.delete rid srs
                        return (nsrs, callback) 
                -- send reply: we're done
                unless syncing $ sendToMaster slaveZmqSocket $ RepDone rev
                return rev
            else do 
                sendToMaster slaveZmqSocket RepError
                error $ "Replication failed at revision " ++ show rev ++ " -> " ++ show nr
                return nr
            where decodeEvent ev = case runGet safeGet ev of
                                Left str -> error str
                                Right val -> val

repCheckpoint :: SlaveState st -> Revision -> IO ()
repCheckpoint SlaveState{..} rev = do
    debug "Got Checkpoint request."
    withMVar slaveRevision $ \nr ->
        -- create checkpoint
        createCheckpoint slaveLocalState


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
            let callback = do
                    hd <- scheduleUpdate slaveLocalState event 
                    void $ forkIO $ putMVar result =<< takeMVar hd
            return $ M.insert reqId callback srs
        return result

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
        -- wait replication chan
        debug "Waiting for repChan to empty."
        mtid <- myThreadId
        putMVar slaveRepThreadId mtid
        -- kill handler threads
        debug "Killing request handler."
        withMVar slaveReqThreadId killThread
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
              , createCheckpoint   = undefined
              , createArchive      = undefined
              , closeAcidState     = liberateState slaveState 
              , acidSubState       = mkAnyState slaveState
              }
