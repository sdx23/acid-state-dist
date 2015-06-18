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
import Control.Monad (forever, when, unless, forM_, liftM, liftM2)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (readTVar)
import qualified Control.Concurrent.Event as E

import System.ZMQ4 (Context, Socket, Router(..), Receiver, Flag(..),
                    context, term, socket, close, 
                    bind, unbind,
                    send, receive)

import qualified Data.Map as M
import Data.Map (Map)
import qualified Data.ByteString.Lazy.Char8 as CSL
import qualified Data.ByteString.Char8 as CS
import Data.ByteString.Char8 (ByteString)

-- auto imports following - need to be cleaned up
import Control.Concurrent.MVar(MVar, modifyMVar, modifyMVar_, withMVar, newMVar)

--------------------------------------------------------------------------------

data MasterState st 
    = MasterState { localState :: AcidState st
                  , nodeStatus :: MVar NodeStatus
                  , masterRevision :: MVar NodeRevision
                  , repDone :: E.Event
                  , zmqContext :: Context
                  , zmqAddr :: String
                  , zmqSocket :: Socket Router
                  } deriving (Typeable)

type NodeIdentity = ByteString
type NodeStatus = Map NodeIdentity NodeRevision
        
-- | The replication handler on master node. Does
--      o handle receiving requests from nodes,
--      o answering as needed (old updates),
--      o bookkeeping on node states. 
masterRepHandler :: (Typeable st) => MasterState st -> IO ()
masterRepHandler masterState@MasterState{..} = do
        let loop = do
                -- take one frame
                (ident, msg) <- receiveFrame zmqSocket
                -- handle according frame contents
                case msg of
                    -- New Slave joined.
                    NewSlave r -> do
                        -- todo: the state should be locked at this point to avoid losses(?)
                        pastUpdates <- getPastUpdates localState r
                        connectNode masterState ident pastUpdates
                    -- Slave is done replicating.
                    RepDone r -> updateNodeStatus masterState ident r
                    -- Slave sends an Udate.
                    -- todo: not yet
                    -- Slave quits.
                    SlaveQuit -> removeFromNodeStatus nodeStatus ident
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ show msg
                -- loop around
                debug "loop iteration"
                loop
        loop


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
    modifyMVar_ nodeStatus $ \ns -> 
        withMVar masterRevision $ \mr -> do
            -- todo: there should be a fancy way to do this
            when (M.findWithDefault 0 ident ns /= (mr - 1)) $
                error $ "Invalid increment of node status " ++ show ns ++ " -> " ++ show mr
            -- todo: checks sensible?
            let rs = M.adjust (+1) ident ns
            when (allNodesDone mr rs) $ do
                E.set repDone
                debug $ "All nodes done replicating " ++ show mr
            return rs
            where allNodesDone mrev = M.fold (\v t -> (v == mrev) && t) True

-- | Connect a new Slave by getting it up-to-date,
--   i.e. send all past events as Updates.
--   This temporarily blocks all other communication.
-- todo: updates received by slaves are problematic here!
connectNode :: MasterState st -> NodeIdentity -> [(Int, Tagged CSL.ByteString)] -> IO ()
connectNode MasterState{..} i pastUpdates = 
    withMVar masterRevision $ \mr -> 
        modifyMVar_ nodeStatus $ \ns -> do
            forM_ pastUpdates $ \(r, u) -> do
                sendUpdate zmqSocket r u i
                (ident, msg) <- receiveFrame zmqSocket
                when (ident /= i) $ error "received message not from the new node"
                -- todo: also check increment validity
            return $ M.insert i mr ns 

-- | Send one (encoded) Update to a Slave.
sendUpdate :: Socket Router -> Int -> Tagged CSL.ByteString -> NodeIdentity -> IO ()
sendUpdate sock revision update ident = do
    send sock [SendMore] ident
    send sock [SendMore] CS.empty
    send sock [] $ encode $ DoRep revision update
    

-- | Receive one Frame. A Frame consists of three messages: 
--      sender ID, empty message, and actual content 
receiveFrame :: (Receiver t) => Socket t -> IO (NodeIdentity, SlaveMessage)
receiveFrame sock = do
    ident <- receive sock
    _     <- receive sock
    msg   <- receive sock
    case decode msg of
        -- todo: pass on exceptions
        Left str -> error $ "Data.Serialize.decode failed on SlaveMessage: " ++ show msg
        Right smsg -> do
            debug $ "Received from [" ++ show ident ++ "]: "
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
        -- remote
        ctx <- context
        sock <- socket ctx Router
        rd <- E.newSet
        ns <- newMVar M.empty
        let addr = "tcp://127.0.0.1:" ++ show port
        bind sock addr
        let masterState = MasterState { localState = lst
                                      , nodeStatus = ns
                                      , masterRevision = rev
                                      , repDone = rd
                                      , zmqContext = ctx
                                      , zmqAddr = addr
                                      , zmqSocket = sock
                                      }
        forkIO $ masterRepHandler masterState
        return $ toAcidState masterState

-- | Close the master state.
closeMasterState :: MasterState st -> IO ()
closeMasterState MasterState{..} = do
        debug "Closing master state."
        -- wait all nodes done
        -- todo^ - not necessary for now
        -- cleanup zmq
        unbind zmqSocket zmqAddr 
        close zmqSocket
        term zmqContext
        -- cleanup local state
        closeAcidState localState

-- | Update on master site.
-- todo: this implementation is only valid for Slaves not sending Updates.
scheduleMasterUpdate :: UpdateEvent event => MasterState (EventState event) -> event -> IO (MVar (EventResult event))
scheduleMasterUpdate masterState event = do
        withMVar (masterRevision masterState) $ debug . (++) "Master Update from rev " . show 
        -- do local Update
        res <- scheduleUpdate (localState masterState) event
        modifyMVar_ (masterRevision masterState) (return . (+1))
        -- sent Update to Slaves
        E.clear $ repDone masterState
        sendUpdateSlaves masterState event
        -- wait for Slaves finish replication
        noTimeout <- E.waitTimeout (repDone masterState) (500*1000)
        unless noTimeout $ do
            debug "Timeout occurred."
            E.set (repDone masterState)
            removeLaggingNodes masterState
        return res

-- | Remove nodes that were not responsive
removeLaggingNodes :: MasterState st -> IO ()
removeLaggingNodes MasterState{..} = 
    withMVar masterRevision $ \mr -> modifyMVar_ nodeStatus $ return . M.filter (== mr) 


-- | Send a new update to all Slaves.
sendUpdateSlaves :: (UpdateEvent e) => MasterState st -> e -> IO ()
sendUpdateSlaves MasterState{..} event = withMVar nodeStatus $ \ns -> do
    let allSlaves = M.keys ns
    let numSlaves = length allSlaves
    debug $ "Sending Update to Slaves, there are " ++ show numSlaves
    let encoded = runPutLazy (safePut event)
    withMVar masterRevision $ \mr -> do
        debug $ "Sending Updates to rev " ++ show mr
        forM_ allSlaves $ \i ->
            sendUpdate zmqSocket mr (methodTag event, encoded) i
    -- if there are no Slaves, replication is already done
    when (numSlaves == 0) $ E.set repDone


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


