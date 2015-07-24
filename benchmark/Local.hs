{-# LANGUAGE TypeFamilies #-}

import Criterion.Main

import Data.Acid

import System.Exit (exitSuccess)

-- common benchmarking stuff
import BenchCommon

main :: IO ()
main = do
    -- init acid
    cleanup "state/Local"
    acid <- openLocalStateFrom "state/Local/m" (IntState 0)

    -- run benchmark
    defaultMain
        [ bench "Local" $ nfIO (masterBench acid)
        , bench "Local-grouped" $ nfIO (masterBenchGrouped acid)
        ]

    -- cleanup
    closeAcidState acid
    exitSuccess
