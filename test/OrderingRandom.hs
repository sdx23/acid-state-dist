{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered

import Control.Monad (when, forM_)
import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar, readMVar)
import System.Exit (exitSuccess, exitFailure)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)
import System.Random (mkStdGen, randomRs)

-- state structures
import NcCommon

-- helpers
delaySec :: Int -> IO ()
delaySec n = threadDelay $ n*1000*1000

cleanup :: FilePath -> IO ()
cleanup path = do
    sp <- doesDirectoryExist path
    when sp $ removeDirectoryRecursive path

-- actual test
randRange :: (Int,Int)
randRange = (100,100000)

numRands :: Int
numRands = 100

slave :: Int ->  MVar [Int] -> MVar () -> MVar () -> IO ()
slave ident res done alldone = do
    let rs = randomRs randRange $ mkStdGen ident :: [Int]
    acid <- enslaveStateFrom ("state/OrderingRandom/s" ++ show ident) "localhost" 3333 (NcState [])
    forM_ (take numRands rs) $ \r -> do
        threadDelay r
        update acid $ NcOpState r
    putMVar done ()
    -- wait for others
    _ <- readMVar alldone
    delaySec ident
    val <- query acid GetState
    putMVar res val
    print $ "slave quit ident " ++ show ident
    closeAcidState acid

main :: IO ()
main = do
    cleanup "state/OrderingRandom"
    acid <- openMasterStateFrom "state/OrderingRandom/m" "127.0.0.1" 3333 (NcState [])
    allDone <- newEmptyMVar
    -- start slaves
    s1Res <- newEmptyMVar
    s1Done <- newEmptyMVar
    _ <- forkIO $ slave 1 s1Res s1Done allDone
    threadDelay 1000 -- zmq-indentity could be the same if too fast
    s2Res <- newEmptyMVar
    s2Done <- newEmptyMVar
    _ <- forkIO $ slave 2 s2Res s2Done allDone
    -- manipulate state on master
    let rs = randomRs randRange $ mkStdGen 23 :: [Int]
    forM_ (take numRands rs) $ \r -> do
        threadDelay r
        update acid $ NcOpState r
    -- wait for slaves
    print "at wait"
    _ <- takeMVar s1Done
    _ <- takeMVar s2Done
    -- signal slaves done
    print "at done"
    putMVar allDone ()
    -- collect results
    print "at collect"
    vs1 <- takeMVar s1Res
    vs2 <- takeMVar s2Res
    vm <- query acid GetState
    -- check results
    print "at results"
    when (vs1 /= vs2) exitFailure
    when (vm /= vs1) exitFailure
    when (vm /= vs2) exitFailure
    closeAcidState acid
    exitSuccess

