{-# LANGUAGE RecordWildCards #-}
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

import System.ZMQ4 (socket, Router, bind, receive, liftIO)

type PortNumber = Int

data RepStatus = Done | Replicating | Cleanup

data MasterState st 
    = MasterState { localState :: AcidState st
                  , repStatus :: IORef RepStatus
                  , repHandler :: Chan
                  }

debug :: String -> IO ()
debug msg = return ()
        -- putStrLn msg
        
-- | The replication handler on master node
masterRepHandler :: Chan -> IORef RepStatus -> String -> IO()
masterRepHandler repHandler repStatus addr = runZMQ $ do
        sock <- socket Router
        bind sock addr
        let loop = do
            -- take one frame - only if there is one, else it'd block
            inputWaiting <- poll 10 [Sock sock [In] Nothing]
            unless (null $ head inputWaiting) $ do
                ident <- receive sock
                _ <- receive sock
                msg <- receive sock
                -- now handle received stuff
                return ()
            -- handle send events
            
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
  problem: the receiving loop may block the proactive sending
  solution: before receiving, check whether there is something

  this is still ugly, as it is polling. Why can't we do something reactive?
    not use zmq-monadic but hand out the socket to threads doing 1) and 2).
    there may then be "write" collisions. use locking?
-}



-- | Open the master state.
openMasterState :: (IsAcidic st, Typeable st) =>
               PortNumber   -- ^ port to bind to
            -> st           -- ^ initial state 
            -> IO (AcidState st)
openMasterState port initialState = do
        debug "opening master state"
        -- remote
        rs <- newIORef
        rh <- newChan
        let addr = "tcp://127.0.0.1:" ++ show port
        forkIO $ masterRepHandler rh rs addr 
        -- local
        lst <- openLocalState initialState
        return $ toAcidState MasterState { localState = lst
                                         , repStatus = rs
                                         , repHandler = rh
                                         }

-- | Close the master state.
closeMasterState :: MasterState -> IO ()
closeMasterState MasterState{..} = do
        debug "closing master state"
        closeAcidState localState

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
              , acidSubState       = acidSubState $ localState master
              }

