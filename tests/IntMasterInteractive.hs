{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered
import Data.SafeCopy
import Data.Typeable

import Control.Monad (forever, forM_)
import System.Exit (exitSuccess)

-- state structures
import IntCommon

-- actual test
main :: IO ()
main = do
    acid <- openMasterState 3333 (IntState 0)
    putStrLn usage
    forever $ do
        input <- getLine
        case input of
            ('x':_) -> do
                putStrLn "Bye!"
                closeAcidState acid
                exitSuccess
            ('c':_) -> do
                createCheckpoint acid
                putStrLn "Checkpoint generated."
            ('q':_) -> do
                val <- query acid GetState
                putStrLn $ "Current value: " ++ show val
            ('u':val) -> do
                update acid (SetState (read val :: Int))
                putStrLn "State updated."
            ('i':_) -> update acid IncrementState >> putStrLn "State incremented."
            ('k':_) -> do
                forM_ [1..1000] $ const $ update acid IncrementState
                putStrLn "State incremented 1k times."
            _ -> putStrLn "Unknown command." >> putStrLn usage


usage :: String
usage = "Possible commands:\
        \\n  x    exit\
        \\n  c    checkpoint\
        \\n  q    query the state\
        \\n  u v  update to value v\
        \\n  i    increment\
        \\n  k    increment 1000 times"
