{-# LANGUAGE TypeFamilies #-}

import Data.Acid
import Data.Acid.Centered
import Data.SafeCopy
import Data.Typeable

import Control.Monad (forever, replicateM_)
import System.Exit (exitSuccess)

-- state structures
import IntCommon

-- actual test
main :: IO ()
main = do
    acid <- enslaveState "localhost" 3333 (IntState 0)
    putStrLn usage
    forever $ do
        input <- getLine
        case input of
            ('x':_) -> do
                putStrLn "Bye!"
                closeAcidState acid
                exitSuccess
            ('q':_) -> do
                val <- query acid GetState
                putStrLn $ "Current value: " ++ show val
            ('u':val) -> do
                update acid (SetState (read val :: Int))
                putStrLn "State updated."
            ('i':_) -> update acid IncrementState
            ('h':_) -> do
                replicateM_ 100 $ update acid IncrementState
                putStrLn "State incremented 100 times."
            _ -> putStrLn "Unknown command." >> putStrLn usage


usage :: String
usage = "Possible commands:\
        \\n  x    exit\
        \\n  q    query the state\
        \\n  u v  update to value v\
        \\n  i    increment\
        \\n  h    increment 100 times"
