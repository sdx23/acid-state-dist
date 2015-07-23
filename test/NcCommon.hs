{-# LANGUAGE DeriveDataTypeable, TemplateHaskell, TypeFamilies #-}

module NcCommon where

import Data.Acid
import Data.SafeCopy
import Data.Typeable

import Control.Monad.Reader (ask)
import Control.Monad.State (put, get)

-- encapsulate some integers

data NcState = NcState [Int]
    deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''NcState)

-- transactions

getState :: Query NcState [Int]
getState = do
    NcState val <- ask
    return val

ncOpState :: Int -> Update NcState ()
ncOpState x = do
    NcState val <- get
    put $ NcState $ ncOp x val

ncOp :: Int -> [Int] -> [Int]
ncOp x v
    | length v < 20 = x : v
    | otherwise     = x : shorten
    where shorten = reverse $ (r !! 1 - r !! 2 + r !! 3):drop 3 r
          r = reverse v

$(makeAcidic ''NcState ['getState, 'ncOpState])
