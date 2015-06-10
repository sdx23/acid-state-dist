{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Control.Monad.IO.Class (MonadIO)
import System.IO
import System.ZMQ4.Monadic
import qualified Data.ByteString.Char8 as CS
import qualified Data.Map.Strict as Map
import Control.Concurrent (forkIO, threadDelay)
import Data.IORef (newIORef, modifyIORef, modifyIORef', readIORef, IORef)
import Data.Maybe (fromMaybe)


addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    -- current revision and updates
    curRev <- newIORef 1
    nodesRev <- newIORef Map.empty
    forkIO $ forever $ do
        threadDelay $ 500 * 1000
        modifyIORef' curRev (+1)
    -- worker for distributing updates
    runZMQ $ do
        sock <- socket Router
        bind sock addr
        -- now loop and:
        --      o update nodes not on current revision
        --      o receive node responses and update revision list 
        forever $ do
            -- update revision list
            re <- poll 100 [Sock sock [In] Nothing]
            unless (null $ head re) $ do
                ident <- receive sock
                _ <- receive sock
                msg <- receive sock
                case CS.head msg of
                    'S' -> addNode nodesRev ident 
                    'D' -> incRevNode nodesRev ident (msgToRev msg)
                    _ -> error $ "unknown message: " ++ show msg
                liftIO $ CS.putStrLn $ CS.append (formatID ident) msg
                liftIO $ hFlush stdout
            -- distribute update
            cr <- liftIO $ readIORef curRev
            nrs <- liftIO $ readIORef nodesRev 
            forM_ (Map.keys nrs) $ \i -> do
                let rev = Map.findWithDefault 0 i nrs
                when (rev < cr) $ sendUpdate sock i (rev + 1)
            --liftIO $ print $ "current " ++ show nrs 
            --liftIO $ print $ "currentr " ++ show cr
        return ()
    where formatID i = CS.cons '[' $ CS.append i "] "

addNode :: (MonadIO m) => IORef (Map.Map CS.ByteString Int) -> CS.ByteString -> m ()
addNode ns i = liftIO $ modifyIORef ns (Map.insert i 0)

incRevNode :: (MonadIO m) => IORef (Map.Map CS.ByteString Int) -> CS.ByteString -> Int -> m ()
incRevNode ns i r = liftIO $ modifyIORef ns (Map.adjust (const r) i)

sendUpdate :: Sender t => Socket z t -> CS.ByteString -> Int -> ZMQ z ()
sendUpdate sock ident num = do
    send sock [SendMore] ident
    send sock [SendMore] ""
    send sock [] $ CS.cons 'U' (CS.pack (show (num :: Int)))

msgToRev :: CS.ByteString -> Int
msgToRev m = fst $ fromMaybe (0,"") (CS.readInt $ CS.tail m)
