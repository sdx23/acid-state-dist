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

-- the master
master :: MVar () -> IO ()
master sync = do
    acid <- openMasterStateFrom "state/Slave/m" "127.0.0.1" 3333 (IntState 0)
    takeMVar sync
    closeAcidState acid

main :: IO ()
main = do
    -- init acid
    cleanup "state/Slave"
    sync <- newEmptyMVar
    void $ forkIO $ master sync
    acid <- enslaveStateFrom "state/Slave/s1" "localhost" 3333 (IntState 0)
    delaySec 3

    -- run benchmark
    defaultMain
        [ bench "Slave" $ nfIO (slaveBench acid)
        , bench "Slave-grouped" $ nfIO (slaveBenchGrouped acid)
        ]

    -- cleanup
    putMVar sync ()
    closeAcidState acid
    exitSuccess
