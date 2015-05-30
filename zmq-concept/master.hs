{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import System.IO
import System.ZMQ4.Monadic
import qualified Data.ByteString.Char8 as CS
import Control.Concurrent (forkIO, threadDelay)
import Data.IORef (newIORef, modifyIORef, readIORef)


addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    curRev <- newIORef 0
    forkIO $ forever $ do
        threadDelay $ 500 * 1000
        modifyIORef curRev (+1)
    runZMQ $ do
        sock <- socket Router
        bind sock addr
        forever $ do
            ident <- receive sock
            _ <- receive sock
            msg <- receive sock
            cr <- liftIO $ readIORef curRev
            case CS.head msg of
                'S' -> sendUpdate sock ident 0
                'D' -> sendUpdate sock ident cr
            liftIO $ CS.putStrLn $ CS.append (formatID ident) msg
            liftIO $ hFlush stdout
        return ()
    where formatID i = CS.cons '[' $ CS.append i "] "

sendUpdate sock id num = do
    send sock [SendMore] id
    send sock [SendMore] ""
    send sock [] $ CS.cons 'U' (CS.pack (show (num :: Int)))


