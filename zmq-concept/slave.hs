{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Control.Concurrent (threadDelay)
import System.IO
import System.ZMQ4.Monadic
import qualified Data.ByteString.Char8 as CS

addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    runZMQ $ do
        sock <- socket Req
        connect sock addr
        send sock [] "started"
        forever $ do
            liftIO $ threadDelay 500000
            c <- receive sock 
            liftIO . CS.putStrLn $ c
            send sock [] $ CS.append "Done: " $ c
            liftIO $ hFlush stdout
