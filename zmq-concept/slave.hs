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
        send sock [] "S"
        forever $ do
            liftIO $ threadDelay 500000
            msg <- receive sock 
            case CS.head msg of
                'U' -> do
                    send sock [] $ CS.append "Done: " $ msg
                    liftIO . CS.putStrLn $ CS.append "D" $ CS.tail msg
                    liftIO $ hFlush stdout
                _ -> return ()
