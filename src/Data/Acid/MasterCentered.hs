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

import System.ZMQ4

type PortNumber = Int

data MasterState st 
    = MasterState { localState :: AcidState st
                  , zmqContext :: Context
                  }

openMasterState :: (IsAcidic st, Typeable st) =>
               PortNumber
            -> st
            -> IO (AcidState st)
openMasterState port initialState = do
        -- remote
        ctx <- context
            -- bind to port
            -- fork to wait for connects and handle slaves
            -- as we fork anyways, perhaps use runZMQ in there - but will
            --   it terminate gracefully?
        -- local
        lst <- openLocalState initialState
        return $ toAcidState MasterState { localState = lst
                                         , zmqContext = ctx
                                         }

closeMasterState MasterState{..} = do
        destroy zmqContext
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

