{-# LANGUAGE TypeFamilies #-}

import Criterion.Main

import Data.Acid
import Data.Acid.Centered

import System.Exit (exitSuccess)
import Control.Monad (void)
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)

-- common benchmarking stuff
import BenchCommon

-- the slave
slave :: MVar () -> IO ()
slave sync = do
    acid <- enslaveStateFrom "state/MasterSlave/s1" "localhost" 3333 (IntState 0)
    takeMVar sync
    closeAcidState acid

main :: IO ()
main = do
    -- init acid
    cleanup "state/MasterSlave"
    acid <- openMasterStateFrom "state/MasterSlave/m" "127.0.0.1" 3333 (IntState 0)
    sync <- newEmptyMVar
    void $ forkIO $ slave sync
    delaySec 3

    -- run benchmark
    defaultMain
        [ bench "MasterSlave" $ nfIO (masterBench acid)
        , bench "MasterSlave-grouped" $ nfIO (masterBenchGrouped acid)
        ]

    -- cleanup
    putMVar sync ()
    delaySec 5
    closeAcidState acid
    exitSuccess
