{-# LANGUAGE OverloadedStrings #-}
import System.ZMQ4
import Control.Concurrent 
import Control.Monad
import qualified Data.ByteString.Char8 as CS

receiver = do
    ctx <- context
    sock <- socket ctx Dealer
    --setReceiveHighWM (restrict (100*1000)) sock
    connect sock "tcp://localhost:3335"
    send sock [] "new here"
    forever $ do 
        msg <- receive sock
        CS.putStrLn $ "R " `CS.append` msg
    -- this is important since garbage collection!
    close sock
    term ctx

main = do
    forkIO receiver

    ctx <- context
    sock <- socket ctx Router
    --setReceiveHighWM (restrict (100*1000)) sock
    bind sock "tcp://*:3335"

    ident <- receive sock
    _ <- receive sock

    forever $ do 
        c <- getLine
        case c of
            'v':_ -> forM_ [0..100] $ \i -> do
               send sock [SendMore] ident
               send sock [] $ CS.pack $ show i 
            's':_ -> forM_ [0..1000] $ \i -> do
               send sock [SendMore] ident
               send sock [] $ CS.pack $ show i 
            'b':_ -> forM_ [0..1000000] $ \i -> do
               send sock [SendMore] ident
               send sock [] $ CS.pack $ show i 
            _ -> print "unknown command"

    -- this is important since garbage collection!
    close sock
    term ctx

