{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (void, when)
import Control.Concurrent (forkIO, threadDelay)
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
slave1 :: IO ()
slave1 = do
    acid <- enslaveStateFrom "state/SlaveUpdates/s1" "localhost" 3333 (IntState 0)
    update acid (SetState 23)
    val <- query acid GetState
    closeAcidState acid
    when (val /= 23) $ putStrLn "Slave 1 hasn't got value." >> exitFailure

slave2 :: IO ()
slave2 = do
    acid <- enslaveStateFrom "state/SlaveUpdates/s2" "localhost" 3333 (IntState 0)
    delaySec 5
    val <- query acid GetState
    closeAcidState acid
    when (val /= 23) $ putStrLn "Slave 2 hasn't got value." >> exitFailure

main :: IO ()
main = do
    cleanup "state/SlaveUpdates"

    acid <- openMasterStateFrom "state/SlaveUpdates/m" "127.0.0.1" 3333 (IntState 0)
    void $ forkIO slave1
    slave2
    val <- query acid GetState
    closeAcidState acid
    when (val /= 23) $ putStrLn "Master hasn't got value." >> exitFailure

    exitSuccess

