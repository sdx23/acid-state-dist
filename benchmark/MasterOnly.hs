{-# LANGUAGE TypeFamilies #-}

import Criterion
import Criterion.Main

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, replicateM_)
import Control.Concurrent (threadDelay)
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

main :: IO ()
main = do
    -- init acid
    cleanup "state/MasterOnly"
    acid <- openMasterStateFrom "state/MasterOnly/m" "127.0.0.1" 3333 (IntState 0)
    delaySec 2

    -- run benchmark
    defaultMain
        [ bench "MasterOnly" $ nfIO (masterBench acid)
        ]

    -- cleanup
    closeAcidState acid
    exitSuccess
