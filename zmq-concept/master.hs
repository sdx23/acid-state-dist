{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import System.IO
import System.ZMQ4.Monadic
import qualified Data.ByteString.Char8 as CS

addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    runZMQ $ do
        sock <- socket Router
        bind sock addr
        forM_ [1..] $ \i -> do
            ident <- receive sock
            _ <- receive sock
            msg <- receive sock
            send sock [SendMore] ident
            send sock [SendMore] ""
            send sock [] (CS.pack (show (i :: Int)))

            liftIO $ CS.putStrLn $ CS.append (formatID ident) msg
            liftIO $ hFlush stdout
        return ()
    where formatID i = CS.cons '[' $ CS.append i "] "
