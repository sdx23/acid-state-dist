{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE ScopedTypeVariables#-}

import Control.Monad.Reader
import Control.Monad.State
import Data.Acid
import Data.Acid.Centered
import Data.SafeCopy
import Data.Typeable
import Control.Exception

------------------------------------------------------
-- The Haskell structure that we want to encapsulate

data HelloWorldState = HelloWorldState String
    deriving (Show, Typeable)

$(deriveSafeCopy 0 'base ''HelloWorldState)

------------------------------------------------------
-- The transaction we will execute over the state.

writeState :: String -> Update HelloWorldState ()
writeState newValue
    = put (HelloWorldState newValue)

queryState :: Query HelloWorldState String
queryState = do HelloWorldState string <- ask
                return string

$(makeAcidic ''HelloWorldState ['writeState, 'queryState])

main :: IO ()
main = do
    acid <- enslaveState "localhost"  3333 (HelloWorldState "Hello world")
    handle (\(e :: SomeException) -> putStrLn ("Exceptionally shut down Slave, due to: " ++ show e) >> closeAcidState acid) $ do
        putStrLn "Possible commands: x for exit; q for query; uString for update;"
        let loop = do
              input <- getLine
              case input of
                  ('x':_) -> do
                      putStrLn "Bye!"
                      closeAcidState acid
                  ('q':_) -> do
                      string <- query acid QueryState
                      putStrLn $ "The state is: " ++ string
                      loop
                  ('u':str) -> do
                      update acid (WriteState str)
                      putStrLn "The state has been modified!"
                      loop
                  _ -> do
                      putStrLn $ "Unknown command " ++ input
                      loop
        loop
