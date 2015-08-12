{-# LANGUAGE TypeFamilies, ScopedTypeVariables #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when)
import Control.Concurrent (threadDelay)
import System.Exit (exitSuccess, exitFailure)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)
import Control.Exception (catch, SomeException)

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
    acid <- enslaveStateFrom "state/SyncTimeout/s1" "localhost" 3333 (IntState 0)
    delaySec 11     -- SyncTimeout happens at 10 seconds
    closeAcidState acid

main :: IO ()
main = do
    cleanup "state/SyncTimeout"
    catch slave $ \(e :: SomeException) ->
        if show e == "Data.Acid.Centered.Slave: Took too long to sync. Timeout."
            then exitSuccess
            else exitFailure

