{-# LANGUAGE TypeFamilies #-}

import Criterion
import Criterion.Main

import Data.Acid

import Control.Monad (when, replicateM_)
import System.Exit (exitSuccess)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)

-- state structures
import IntCommon

-- helpers
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
    cleanup "state/Local"
    acid <- openLocalStateFrom "state/Local/m" (IntState 0)

    -- run benchmark
    defaultMain
        [ bench "Local" $ nfIO (masterBench acid)
        ]

    -- cleanup
    closeAcidState acid
    exitSuccess
