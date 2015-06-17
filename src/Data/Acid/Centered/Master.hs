{-# LANGUAGE DeriveDataTypeable, RecordWildCards, OverloadedStrings #-}
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
import Data.Serialize (runPutLazy, runPut)

import Data.Acid.Centered.Common

import Control.Concurrent (forkIO)
import Control.Monad (forever, when, forM_)
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

data MasterState st 
    = MasterState { localState :: AcidState st
                  , nodeStatus :: MVar NodeStatus
                  , repDone :: E.Event
                  , zmqContext :: Context
                  , zmqAddr :: String
                  , zmqSocket :: Socket Router
                  } deriving (Typeable)

type NodeIdentity = ByteString
type NodeStatus = Map NodeIdentity Int
        
-- | The replication handler on master node. Does
--      o handle receiving requests from nodes,
--      o answering as needed (old updates),
--      o bookkeeping on node states. 
masterRepHandler :: (Typeable st) => MasterState st -> IO ()
masterRepHandler MasterState{..} = do
        let loop = do
                -- take one frame
                (ident, msg) <- receiveFrame zmqSocket
                -- handle according frame contents
                case CS.head msg of
                    -- a _N_ew slave node
                    'N' -> do
                        -- todo: the state should be locked at this point to avoid losses
                        oldUpdates <- getPastUpdates localState
                        connectNode zmqSocket nodeStatus ident oldUpdates
                    -- Update was _D_one 
                    'D' -> updateNodeStatus nodeStatus repDone ident msg cr
                    -- Slave sends an _U_date
                    'U' -> undefined -- todo: not yet
                    -- no other messages possible
                    _ -> error $ "Unknown message received: " ++ CS.unpack msg
                -- loop around
                debug "loop iteration"
                loop
        loop
        where cr = undefined :: Int

-- | Fetch past Updates from FileLog for replication.
getPastUpdates :: (Typeable st) => AcidState st -> IO [Tagged CSL.ByteString]
getPastUpdates state = readEntriesFrom (localEvents $ downcast state) 0


-- | Update the NodeStatus after a node has replicated an Update.
updateNodeStatus :: MVar NodeStatus -> E.Event -> NodeIdentity -> ByteString -> Int -> IO ()
updateNodeStatus nodeStatus rDone ident msg cr = 
    modifyMVar_ nodeStatus $ \ns -> do
        -- todo: there should be a fancy way to do this
        --when (M.findWithDefault 0 ident ns /= (cr - 1)) $ error "Invalid increment of node status."
        let rs = M.adjust (+1) ident ns
        when (allNodesDone rs) $ do
            E.set rDone
            debug $ "all nodes done with " ++ show cr
        return rs
        where 
            allNodesDone = M.fold (\v t -> (v == cr) && t) True

-- | Connect a new Slave by getting it up-to-date,
--   i.e. send all past events as Updates.
--   This temporarily blocks all other communication.
-- todo: updates received by slaves are problematic here!
connectNode :: Socket Router -> MVar NodeStatus -> NodeIdentity -> [Tagged CSL.ByteString] -> IO ()
connectNode sock nodeStatus i oldUpdates = 
    modifyMVar_ nodeStatus $ \ns -> do
        forM_ oldUpdates $ \u -> do
            sendUpdate sock (encoded u) i
            (ident, msg) <- receiveFrame sock
            when (ident /= i) $ error "received message not from the new node"
            -- todo: also check increment validity
        return $ M.insert i rev ns 
    where  
        encoded = undefined
        rev = length oldUpdates

encodeUpdate :: (UpdateEvent e) => e -> ByteString
encodeUpdate event = runPut (safePut event)

-- | Send one (encoded) Update to a Slave.
sendUpdate :: Socket Router -> ByteString -> NodeIdentity -> IO ()
sendUpdate sock update ident = do
    send sock [SendMore] ident
    send sock [SendMore] ""
    send sock [] $ 'U' `CS.cons` encoded
    where 
        encoded = undefined -- encode Tag and BS of update
    

-- | Receive one Frame. A Frame consists of three messages: 
--      sender ID, empty message, and actual content 
receiveFrame :: (Receiver t) => Socket t -> IO (NodeIdentity, ByteString)
receiveFrame sock = do
    ident <- receive sock
    _     <- receive sock
    msg   <- receive sock
    debug $ "received from [" ++ show ident ++ "]: " ++ show msg
    return (ident, msg)
    

-- | Open the master state.
openMasterState :: (IsAcidic st, Typeable st) =>
               PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state 
            -> IO (AcidState st)
openMasterState port initialState = do
        debug "opening master state"
        -- local
        lst <- openLocalState initialState
        -- remote
        ctx <- context
        sock <- socket ctx Router
        rd <- E.newSet
        ns <- newMVar M.empty
        let addr = "tcp://127.0.0.1:" ++ show port
        bind sock addr
        let masterState = MasterState { localState = lst
                                      , nodeStatus = ns
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
    -- sent Update to Slaves
    E.clear $ repDone masterState
    sendUpdateSlaves masterState event
    -- wait for Slaves finish replication
    E.wait $ repDone masterState
    return res

sendUpdateSlaves :: (SafeCopy e) => MasterState st -> e -> IO ()
sendUpdateSlaves MasterState{..} event = withMVar nodeStatus $ \ns -> do
    let allSlaves = M.keys ns
    let encoded = runPut (safePut event)
    forM_ allSlaves $ \i -> sendUpdate zmqSocket encoded i


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


