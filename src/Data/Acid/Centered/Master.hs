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
import Control.Monad (forever, when, forM_, liftM, liftM2)
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
                        pastUpdates <- getPastUpdates localState
                        connectNode zmqSocket nodeStatus ident pastUpdates
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
getPastUpdates :: (Typeable st) => AcidState st -> IO [Tagged CSL.ByteString]
getPastUpdates state = readEntriesFrom (localEvents $ downcast state) 0

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
connectNode :: Socket Router -> MVar NodeStatus -> NodeIdentity -> [Tagged CSL.ByteString] -> IO ()
connectNode sock nodeStatus i oldUpdates = 
    modifyMVar_ nodeStatus $ \ns -> do
        forM_ (zip oldUpdates [0..]) $ \(u, r) -> do
            sendUpdate sock r (encode u) i
            (ident, msg) <- receiveFrame sock
            when (ident /= i) $ error "received message not from the new node"
            -- todo: also check increment validity
        return $ M.insert i rev ns 
    where rev = length oldUpdates

encodeUpdate :: (UpdateEvent e) => e -> ByteString
encodeUpdate event = runPut (safePut event)

-- | Send one (encoded) Update to a Slave.
sendUpdate :: Socket Router -> Int -> ByteString -> NodeIdentity -> IO ()
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
    debug $ "received from [" ++ show ident ++ "]: " ++ show msg
    case decode msg of
        -- todo: pass on exceptions
        Left str -> error $ "Data.Serialize.decode failed on SlaveMessage: " ++ show msg
        Right smsg -> return (ident, smsg)

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
        debug "closing master state"
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
        -- do local Update
        res <- scheduleUpdate (localState masterState) event
        modifyMVar_ (masterRevision masterState) $ \mr -> do
            -- sent Update to Slaves
            E.clear $ repDone masterState
            sendUpdateSlaves masterState (mr + 1) event
            return (mr + 1)
        -- wait for Slaves finish replication
        E.wait $ repDone masterState
        return res

-- | Send a new update to all Slaves.
sendUpdateSlaves :: (SafeCopy e) => MasterState st -> Int -> e -> IO ()
sendUpdateSlaves MasterState{..} revision event = withMVar nodeStatus $ \ns -> do
    let allSlaves = M.keys ns
    let encoded = runPut (safePut event)
    forM_ allSlaves $ \i ->
        sendUpdate zmqSocket revision encoded i


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


