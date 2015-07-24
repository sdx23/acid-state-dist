{-# LANGUAGE TypeFamilies #-}

import Criterion.Main

import Data.Acid
import Data.Acid.Centered

import System.Exit (exitSuccess)

-- common benchmarking stuff
import BenchCommon

main :: IO ()
main = do
    -- init acid
    cleanup "state/MasterOnly"
    acid <- openMasterStateFrom "state/MasterOnly/m" "127.0.0.1" 3333 (IntState 0)
    delaySec 3

    -- run benchmark
    defaultMain
        [ bench "MasterOnly" $ nfIO (masterBench acid)
        , bench "MasterOnly-grouped" $ nfIO (masterBenchGrouped acid)
        ]

    -- cleanup
    closeAcidState acid
    exitSuccess
