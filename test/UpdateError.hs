{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, void)
import Control.Concurrent (threadDelay, forkIO)
import System.Exit (exitSuccess, exitFailure)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)
import Control.Exception (finally)

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
    acid <- enslaveStateFrom "state/UpdateError/s1" "localhost" 3333 (IntState 0)
    delaySec 1
    finally     -- the update fails; if not it's an error
        (update acid (SetState (error "fail s1")) >> exitFailure)
        (closeAcidState acid)

slave2 :: IO ()
slave2 = do
    acid <- enslaveStateFrom "state/UpdateError/s2" "localhost" 3333 (IntState 0)
    delaySec 1
    finally     -- the update fails; if not it's an error
        (update acid (error "fail s2" :: IncrementState) >> exitFailure)
        (closeAcidState acid)

main :: IO ()
main = do
    cleanup "state/UpdateError"
    acid <- openMasterStateFrom "state/UpdateError/m" "127.0.0.1" 3333 (IntState 0)
    update acid (SetState 23)
    void $ forkIO slave1
    void $ forkIO slave2
    delaySec 2
    closeAcidState acid
    exitSuccess

