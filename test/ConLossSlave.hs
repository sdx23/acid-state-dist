{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, replicateM_)
import Control.Concurrent (threadDelay)
import System.Exit (exitSuccess, exitFailure)
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

-- actual test
main :: IO ()
main = do
    cleanup "state/ConLoss/s1"
    acid <- enslaveStateFrom "state/ConLoss/s1" "localhost" 3333 (IntState 0)
    replicateM_ 20 $ do
        delaySec 1
        --update acid IncrementState
        v <- query acid GetState
        putStrLn $ "Current state at Slave: " ++ show v
    closeAcidState acid

