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

import System.ZMQ4 (Context, Socket, Dealer(..), Receiver, Flag(..),
                    context, term, socket, close, 
                    connect, disconnect,
                    send, receive)

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newMVar, newEmptyMVar, 
                                modifyMVar, modifyMVar_,
                                takeMVar, putMVar)
import Control.Monad (forever, void,
                      when, unless
                     )
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)
import Control.Concurrent.Event (Event)
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
                 , slaveSyncDone :: Event
                 , slaveRevision :: MVar NodeRevision
                 , slaveRequests :: MVar SlaveRequests
                 , slaveZmqContext :: Context
                 , slaveZmqAddr :: String
                 , slaveZmqSocket :: Socket Dealer
                 } deriving (Typeable)

-- | Memory of own Requests sent to Master.
type SlaveRequests = Map RequestID (IO ())

-- | One Update + Metainformation to replicate.
type SlaveRepItem = (Revision, Maybe RequestID, Tagged CSL.ByteString)

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
        sock <- socket ctx Dealer
        srs <- newMVar M.empty
        repChan <- newChan
        syncDone <- Event.new
        let addr = "tcp://" ++ address ++ ":" ++ show port
        connect sock addr
        sendToMaster sock $ NewSlave lrev
        let slaveState = SlaveState { slaveLocalState = lst
                                    , slaveRepChan = repChan
                                    , slaveSyncDone = syncDone
                                    , slaveRevision = rev
                                    , slaveRequests = srs
                                    , slaveZmqContext = ctx
                                    , slaveZmqAddr = addr
                                    , slaveZmqSocket = sock
                                    }
        forkIO $ slaveRequestHandler slaveState 
        forkIO $ slaveReplicationHandler slaveState 
        return $ slaveToAcidState slaveState 

-- | Replication handler of the Slave. 
slaveRequestHandler :: SlaveState st -> IO ()
slaveRequestHandler slaveState@SlaveState{..} = forever $ do
        msg <- receive slaveZmqSocket
        case decode msg of
            Left str -> error $ "Data.Serialize.decode failed on MasterMessage: " ++ show msg
            Right mmsg -> case mmsg of
                    -- We are sent an Update to replicate.
                    DoRep r i d -> queueUpdate slaveState (r, i, d)
                    -- We are sent an Update to replicate for synchronization.
                    DoSyncRep r d -> replicateSyncUpdate slaveState r d 
                    -- Master done sending all synchronization Updates.
                    SyncDone -> debug "Sync Done." >> Event.set slaveSyncDone
                    -- We are requested to Quit.
                    MasterQuit -> undefined -- todo: how get a State that wasn't closed closed?
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ show mmsg

-- | Queue Updates into Chan for replication.
queueUpdate :: SlaveState st -> SlaveRepItem -> IO ()
queueUpdate SlaveState{..} repItem@(rev, _, _) = do
        debug $ "Queuing Update with revision " ++ show rev
        writeChan slaveRepChan repItem

-- | Replicates content of Chan.
slaveReplicationHandler slaveState@SlaveState{..} = do
        noTimeout <- Event.waitTimeout slaveSyncDone $ 10*1000*1000
        unless noTimeout $ error "Slave took too long to sync, ran into timeout."
        forever $ do
            repItem <- readChan slaveRepChan
            replicateUpdate slaveState repItem False

-- | Replicate Sync-Updates directly.
replicateSyncUpdate slaveState rev event = replicateUpdate slaveState (rev, Nothing, event) True

-- | Replicate an Update as requested by Master.
--   Updates that were requested by this Slave are run locally and the result
--   put into the MVar in SlaveRequests.
--   Other Updates are just replicated without using the result.
replicateUpdate :: SlaveState st -> SlaveRepItem -> Bool -> IO ()
replicateUpdate SlaveState{..} (rev, reqId, event) syncing = do
        debug $ "Got an Update to replicate " ++ show rev
        modifyMVar_ slaveRevision $ \nr -> case rev - 1 of
            nr -> do
                -- commit / run it locally 
                case reqId of
                    Nothing -> 
                        void $ scheduleColdUpdate slaveLocalState event 
                    Just rid -> modifyMVar slaveRequests $ \srs -> do
                        debug $ "This is the Update for Request " ++ show rid
                        callback <- fromMaybe (error $ "Callback not found: " ++ show rid) (M.lookup rid srs) 
                        let nsrs = M.adjust (\c -> return ()) rid srs
                        return (nsrs, callback) 
                -- send reply: we're done
                unless syncing $ sendToMaster slaveZmqSocket $ RepDone rev
                return rev
            _  -> do 
                sendToMaster slaveZmqSocket RepError
                error $ "Replication failed at revision " ++ show rev ++ " -> " ++ show nr
                return nr
            where decodeEvent ev = case runGet safeGet ev of
                                Left str -> error str
                                Right val -> val

-- | Update on slave site. 
--      The steps are:  
--      - Request Update from Master
--      - Master issues Update with same RequestID
--      - repHandler replicates and puts result in MVar
scheduleSlaveUpdate :: UpdateEvent e => SlaveState (EventState e) -> e -> IO (MVar (EventResult e))
scheduleSlaveUpdate SlaveState{..} event = do
        debug "Update by Slave."
        result <- newEmptyMVar
        modifyMVar_ slaveRequests $ \srs -> do
            let encoded = runPutLazy (safePut event)
            let reqId = if M.null srs then 0 else (+1) $ fst $ M.findMax srs
            sendToMaster slaveZmqSocket $ ReqUpdate reqId (methodTag event, encoded)
            -- todo: could this be more efficient?
            let callback = putMVar result =<< takeMVar =<< scheduleUpdate slaveLocalState event 
            return $ M.insert reqId callback srs
        return result

-- | Send a message to Master.
sendToMaster :: Socket Dealer -> SlaveMessage -> IO ()
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
