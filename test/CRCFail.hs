{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (void, when)
import Control.Concurrent (threadDelay, forkIO)
import System.Exit (exitSuccess, exitFailure)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)

import Control.Exception (handle, SomeException)

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
slave :: AcidState IntState -> IO ()
slave master = handle eHandler $ do
    acid <- enslaveStateFrom "state/CRCFail/s1" "localhost" 3333 (IntState 23)
    -- at this point happens the crc fail - we check for an exception and
    -- thereby determine whether the test was successful
    delaySec 2
    -- this should never be executed
    closeAcidState acid
    exitFailure
    where
        eHandler :: SomeException -> IO ()
        eHandler e = when (show e == "Data.Acid.Centered.Slave: CRC mismatch after sync. Exiting.") $ do
            closeAcidState master
            exitSuccess

main :: IO ()
main = do
    cleanup "state/CRCFail"
    acid <- openMasterStateFrom "state/CRCFail/m" "127.0.0.1" 3333 (IntState 0)
    void $ forkIO $ slave acid
    delaySec 4
    closeAcidState acid
    exitSuccess

