{-# LANGUAGE TypeFamilies #-}

import Criterion
import Criterion.Main

import Data.Acid
import Data.Acid.Centered

import Control.Monad (void, when, replicateM_)
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import System.Exit (exitSuccess)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)

-- state structures
import IntCommon

-- helpers
delaySec :: Int -> IO ()
delaySec n = threadDelay $ n*1000*1000

cleanup :: FilePath -> IO ()
cleanup path = do
    sp <- doesDirectoryExist path
    when sp $ removeDirectoryRecursive path

-- benchmark
masterBench :: AcidState IntState -> IO ()
masterBench acid = replicateM_ 100 $ update acid IncrementState

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
    delaySec 2

    -- run benchmark
    defaultMain
        [ bench "MasterSlave" $ nfIO (masterBench acid)
        ]

    -- cleanup
    putMVar sync ()
    closeAcidState acid
    exitSuccess
