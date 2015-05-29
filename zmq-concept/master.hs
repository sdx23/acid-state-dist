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
            case CS.head msg of
                'S' -> sendUpdate sock ident 0
                'D' -> sendUpdate sock ident i

            liftIO $ CS.putStrLn $ CS.append (formatID ident) msg
            liftIO $ hFlush stdout
        return ()
    where formatID i = CS.cons '[' $ CS.append i "] "

sendUpdate sock id num = do
    send sock [SendMore] id
    send sock [SendMore] ""
    send sock [] $ CS.cons 'U' (CS.pack (show (num :: Int)))


