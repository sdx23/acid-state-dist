{-# LANGUAGE DeriveDataTypeable, RecordWildCards #-}
-----------------------------------------------------------------------------
{- |
  Module      :  Data.Acid.MasterCentered
  Copyright   :  ?

  Maintainer  :  max.voit+hdv@with-eyes.net
  Portability :  ?

  This module provides a replication backend for acid state, centered around 
  the master. Thus in case of partitions no updates may be accepted and the
  system blocks.

-}
{- big chunks still todo:
    o master part
    o slave part
    o checkpoints / archives
    o authentification
    o encryption
-}
module Data.Acid.MasterCentered
    (
    -- * Master / Slave
      openMasterState
    , enslaveState
    ) where

import Data.Typeable
import Data.SafeCopy

import Data.Acid
import Data.Acid.Abstract -- not exported by acid-state, export and reinstall
import Data.Acid.Advanced
import Data.Acid.Local

import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent (forkIO)

import System.ZMQ4 (Context, Socket, Router(..), context, term, socket, close, 
                    bind, unbind, send, receive, sendMulti, receiveMulti)
import Data.IORef (IORef, newIORef)
import qualified Data.Map as M
import Data.Map (Map)

-- auto imports following - need to be cleaned up
import Control.Monad.IO.Class(liftIO)
import Control.Concurrent.MVar(MVar)

type PortNumber = Int

data RepStatus = Done | Replicating | Cleanup

type NodeStatus = Map String Int

data MasterState st 
    = MasterState { localState :: AcidState st
                  , nodeStatus :: IORef NodeStatus
                  , repStatus :: IORef RepStatus
                  , zmqContext :: Context
                  , zmqAddr :: String
                  , zmqSocket :: Socket Router
                  } deriving (Typeable)

debug :: String -> IO ()
debug = putStrLn 
        
-- | The replication handler on master node. Does
--      o handle receiving requests from nodes,
--      o answering as needed (old updates),
--      o bookkeeping on node states. 
masterRepHandler :: Socket Router -> IORef RepStatus -> IO ()
masterRepHandler sock repStatus = do
        let loop = do
                -- take one frame
                ident <- receive sock
                _ <- receive sock
                msg <- receive sock
                debug $ "from [" ++ show ident ++ "]: " ++ show msg
                -- now handle received stuff
                
                -- loop around
                liftIO $ debug "loop iteration"
                loop
        loop
{- what do we need to do in the zmq part?
  there is two things:
    1) receiving messages from slave nodes
        - may change repStatus
        - may need to send out rep requests
    2) sending messages proactively, due to an update
 not use zmq-monadic but hand out the socket to threads doing 1) and 2).
    there may then be "write" collisions, but not with sendMulti
-}



-- | Open the master state.
openMasterState :: (IsAcidic st, Typeable st) =>
               PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state 
            -> IO (AcidState st)
openMasterState port initialState = do
        debug "opening master state"
        -- remote
        ctx <- context
        sock <- socket ctx Router
        rs <- newIORef Done
        ns <- newIORef M.empty
        let addr = "tcp://127.0.0.1:" ++ show port
        bind sock addr
        forkIO $ masterRepHandler sock rs 
        -- local
        lst <- openLocalState initialState
        return $ toAcidState MasterState { localState = lst
                                         , nodeStatus = ns
                                         , repStatus = rs
                                         , zmqContext = ctx
                                         , zmqAddr = addr
                                         , zmqSocket = sock
                                         }

-- | Close the master state.
closeMasterState :: MasterState st -> IO ()
closeMasterState MasterState{..} = do
        debug "closing master state"
        -- wait all nodes done
        -- cleanup zmq
        unbind zmqSocket zmqAddr 
        close zmqSocket
        term zmqContext
        -- cleanup local state
        closeAcidState localState

-- | Update on master site.
scheduleMasterUpdate :: UpdateEvent event => MasterState (EventState event) -> event -> IO (MVar (EventResult event))
scheduleMasterUpdate masterState event = do
    -- sent Update to Slaves
    sendUpdateSlaves
    -- do local Update
    scheduleUpdate $ localState masterState
    -- wait for Slaves finish replication

sendUpdateSlaves master = do
    debug "sending update to slaves"
    let salves = allNodes $ nodeStatus master 
    sendMulti
    
allNodes ns = M.keys ns

enslaveState = undefined

toAcidState :: IsAcidic st => MasterState st -> AcidState st
toAcidState master 
  = AcidState { _scheduleUpdate    = scheduleUpdate $ localState master
              , scheduleColdUpdate = scheduleColdUpdate $ localState master
              , _query             = query $ localState master
              , queryCold          = queryCold $ localState master
              , createCheckpoint   = createCheckpoint $ localState master
              , createArchive      = createArchive $ localState master
              , closeAcidState     = closeMasterState master 
              , acidSubState       = mkAnyState master
              }

