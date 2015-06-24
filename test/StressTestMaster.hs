{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# LANGUAGE TypeFamilies       #-}

module Main (main) where

import           Data.Acid
import           Data.Acid.Advanced   (groupUpdates)
import           Data.Acid.Centered.Master

import           Control.Monad.Reader
import           Control.Monad.State
import           Data.SafeCopy
import           System.Environment
import           System.IO
import           System.Exit

import           Data.Typeable

------------------------------------------------------
-- The Haskell structure that we want to encapsulate

data StressState = StressState !Int
    deriving (Typeable)

$(deriveSafeCopy 0 'base ''StressState)

------------------------------------------------------
-- The transaction we will execute over the state.

pokeState :: Update StressState ()
pokeState = do StressState i <- get
               put (StressState (i+1))

queryState :: Query StressState Int
queryState = do StressState i <- ask
                return i

clearState :: Update StressState ()
clearState = put $ StressState 0

$(makeAcidic ''StressState ['pokeState, 'queryState, 'clearState])

------------------------------------------------------
-- This is how AcidState is used:

main :: IO ()
main = do acid <- openMasterState 3333 (StressState 0)
          putStrLn "Possible commands: x for exit; q for query; p for poke;"
          forever $ do
              input <- getLine
              case input of
                    ('x':_) -> do
                        putStrLn "Bye!"
                        closeAcidState acid
                        exitSuccess
                    ('q':_) -> do
                        n <- query acid QueryState
                        putStrLn $ "State value: " ++ show n
                    ('p':_) -> do
                        putStr "Issuing 1k transactions... "
                        hFlush stdout
                        groupUpdates acid (replicate 1000 PokeState)
                        putStrLn "Done"
                    _ -> putStrLn $ "Unknwon command " ++ input
