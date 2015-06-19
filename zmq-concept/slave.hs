{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Control.Concurrent (threadDelay)
import System.IO
import System.ZMQ4.Monadic
import qualified Data.ByteString.Char8 as CS
import Data.IORef (newIORef, modifyIORef, readIORef)
import Data.Maybe (fromMaybe)

addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    myRev <- newIORef 0
    runZMQ $ do
        sock <- socket Dealer
        connect sock addr
        send sock [] "S"
        forever $ do
            -- liftIO $ threadDelay 100000
            msg <- receive sock 
            mcr <- liftIO $ readIORef myRev
            case CS.head msg of
                'U' -> do
                    let nr = msgToRev msg
                    if (nr == mcr + 1) || (nr == mcr) then do
                        send sock [] $ CS.cons 'D' $ CS.tail msg
                        if nr == mcr then
                            liftIO $ print "W: ignoring increment which is none"
                        else do
                            liftIO $ modifyIORef myRev (+1)
                            liftIO . CS.putStrLn $ CS.append "D" $ CS.tail msg
                            liftIO $ hFlush stdout
                    else
                        error $ "E: invalid revision increment " ++ show mcr ++ " -> " ++ show nr
                        -- when coordinator keeps track anyway, this
                        -- shouldn't happen - but is it sensible?
                _ -> return ()

msgToRev m = fst $ fromMaybe (0,"") (CS.readInt $ CS.tail m)

