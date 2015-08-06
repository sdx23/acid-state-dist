{-# LANGUAGE DeriveDataTypeable, TemplateHaskell, TypeFamilies #-}

module IntCommon where

import Data.Acid
import Data.SafeCopy
import Data.Typeable

import Control.Monad.Reader (ask)
import Control.Monad.State (put, get)


-- encapsulate some integers

data IntState = IntState Int
    deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''IntState)

-- transactions

setState :: Int -> Update IntState ()
setState value = put (IntState value)

setStateEven :: Int -> Update IntState Bool
setStateEven value = if even value
    then put (IntState value) >> return True
    else return False

getState :: Query IntState Int
getState = do
    IntState val <- ask
    return val

incrementState :: Update IntState ()
incrementState = do
    IntState val <- get
    put (IntState (val + 1))

$(makeAcidic ''IntState ['setState, 'setStateEven, 'getState, 'incrementState])
