{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, replicateM, void)
import Control.Concurrent (threadDelay, forkIO)
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
    acid <- enslaveRedStateFrom "state/NReplication/s1" "localhost" 3333 (IntState 0)
    delaySec 2
    val <- query acid GetState
    closeAcidState acid
    when (val /= 23) $ putStrLn "Slave hasn't got value." >> exitFailure

main :: IO ()
main = do
    cleanup "state/NReplication"
    acid <- openRedMasterStateFrom "state/NReplication/m" "127.0.0.1" 3333 2 (IntState 0)
    void $ forkIO $ delaySec 2 >> slave
    void $ forkIO $ update acid (SetState 23)      -- this update blocks
    vals <- replicateM 5 $ do
        delaySec 1
        query acid GetState
    closeAcidState acid
    -- queries before the slave joined yield the old state, only after joining
    -- the update is accepted
    if head vals == 0 && last vals == 23
        then exitSuccess
        else exitFailure

