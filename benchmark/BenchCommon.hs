{-# LANGUAGE DeriveDataTypeable, TemplateHaskell, TypeFamilies #-}

module BenchCommon where

import Data.Acid
import Data.Acid.Advanced (groupUpdates)
import Data.SafeCopy
import Data.Typeable

import Control.Monad.Reader (ask)
import Control.Monad.State (put, get)

import Control.Monad (when, replicateM_)
import Control.Concurrent (threadDelay)
import System.Directory (doesDirectoryExist, removeDirectoryRecursive)

-- encapsulate some integers
data IntState = IntState Int
    deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''IntState)

-- transactions
setState :: Int -> Update IntState ()
setState value = put (IntState value)

getState :: Query IntState Int
getState = do
    IntState val <- ask
    return val

incrementState :: Update IntState ()
incrementState = do
    IntState val <- get
    put (IntState (val + 1))

$(makeAcidic ''IntState ['setState, 'getState, 'incrementState])

-- helpers
delaySec :: Int -> IO ()
delaySec n = threadDelay $ n*1000*1000

cleanup :: FilePath -> IO ()
cleanup path = do
    sp <- doesDirectoryExist path
    when sp $ removeDirectoryRecursive path

-- benchmark
masterBench :: AcidState IntState -> IO ()
masterBench acid = replicateM_ 100 $ update acid IncrementState

masterBenchGrouped :: AcidState IntState -> IO ()
masterBenchGrouped acid = groupUpdates acid (replicate 100 IncrementState)

