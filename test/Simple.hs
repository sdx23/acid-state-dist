{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (void, when)
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
slave :: IO ()
slave = do
    acid <- enslaveStateFrom "state/Simple/s1" "localhost" 3333 (IntState 0)
    delaySec 5
    val <- query acid GetState
    closeAcidState acid
    when (val /= 23) $ putStrLn "Slave hasn't got value." >> exitFailure

main :: IO ()
main = do
    cleanup "state/Simple"
    acid <- openMasterStateFrom "state/Simple/m" "127.0.0.1" 3333 (IntState 0)
    update acid (SetState 23)
    slave
    closeAcidState acid
    exitSuccess

