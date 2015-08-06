{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, void, replicateM_)
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
-- This example demonstrates how exceptions are handled when using the state in
-- a threaded environment: Exceptions in Updates kill the thread that invoked
-- the Update not the Slave.
slave :: IO ()
slave = do
    acid <- enslaveStateFrom "state/Threaded/s1" "localhost" 3333 (IntState 0)
    void $ forkIO $ slaveThread acid
    replicateM_ 7 $ do
        delaySec 1
        putStrLn "s"
    val <- query acid GetState
    putStrLn $ "Slave has value " ++ show val
    closeAcidState acid
    where slaveThread a = do
            delaySec 1
            putStrLn "st"
            delaySec 1
            putStrLn "st"
            update a (SetState (error "not today"))
            replicateM_ 5 $ do
                delaySec 1
                putStrLn "st"

main :: IO ()
main = do
    cleanup "state/Threaded"
    acid <- openMasterStateFrom "state/Threaded/m" "127.0.0.1" 3333 (IntState 0)
    update acid (SetState 23)
    void $ forkIO slave
    replicateM_ 10 $ do
        delaySec 1
        putStrLn "m"
    closeAcidState acid

