{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered
import Data.Acid.Centered.Master (createArchiveGlobally)
import Data.SafeCopy
import Data.Typeable

import Control.Monad (forever, forM_)
import Control.Concurrent
import System.Exit

-- state structures
import IntCommon

delaySec n = threadDelay $ n*1000*1000

-- actual test
master :: IO ()
master = do
    acid <- openMasterState 3333 (IntState 0)
    update acid (SetState 23)
    delaySec 1
    closeAcidState acid

slave :: IO ()
slave = do
    -- TODO other working dir or openStateFrom
    acid <- enslaveState "localhost" 3333 (IntState 0)
    val <- query acid GetState
    closeAcidState acid
    if val == 23 then
        exitSuccess
    else
        exitFailure

main :: IO ()
main = do
    forkIO slave
    delaySec 1
    forkIO master
    exitSuccess

