{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, void)
import Control.Concurrent (threadDelay, forkIO)
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

-- actually interesting stuff
-- This example demonstrates in which way a timeout results in exceptions on a
-- Slave that sends updates.
-- The Slave afterwards also runs into a sync timeout as there is no Master.
slave :: IO ()
slave = do
    acid <- enslaveStateFrom "state/Timeout/s1" "localhost" 3333 (IntState 0)
    putStrLn "Connected. Sending Update."
    void $ forkIO $ do
        res <- update acid (SetStateEven 23)
        putStrLn $ "Update result was " ++ show res
    delaySec 6
    closeAcidState acid

main :: IO ()
main = do
    cleanup "state/Timeout"
    --acid <- openMasterStateFrom "state/Timeout/m" "127.0.0.1" 3333 (IntState 0)
    --closeAcidState acid
    slave
