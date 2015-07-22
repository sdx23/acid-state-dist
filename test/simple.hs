{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered
--import Data.SafeCopy
--import Data.Typeable

import Control.Monad (void)
import Control.Concurrent
import System.Exit

-- state structures
import IntCommon

delaySec :: Int -> IO ()
delaySec n = threadDelay $ n*1000*1000

-- actual test
master :: IO ()
master = do
    acid <- openMasterStateFrom "state/simple/m" "127.0.0.1" 3333 (IntState 0)
    update acid (SetState 23)
    delaySec 10
    closeAcidState acid

slave :: IO ()
slave = do
    acid <- enslaveStateFrom "state/simple/s1" "localhost" 3333 (IntState 0)
    delaySec 2
    val <- query acid GetState
    closeAcidState acid
    if val == 23 then
        exitSuccess
    else
        exitFailure

main :: IO ()
main = do
    void $ forkIO master
    delaySec 1
    slave
    exitSuccess

