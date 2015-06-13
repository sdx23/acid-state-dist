{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Control.Monad.IO.Class (MonadIO)
import System.IO
import System.ZMQ4
import qualified Data.ByteString.Char8 as CS
import qualified Data.Map.Strict as Map
import Control.Concurrent (forkIO, threadDelay)
import Data.IORef (newIORef, modifyIORef, modifyIORef', readIORef, IORef)
import Data.Maybe (fromMaybe)
import Control.Concurrent.MVar


addr :: String
addr = "tcp://127.0.0.1:5000"

main :: IO ()
main = do
    -- current revision and updates
    curRev <- newMVar 1
    nodesRev <- newIORef Map.empty
    -- no monadic zmq, share socket
    ctx <- context
    sock <- socket ctx Router
    bind sock addr

    -- /random/ updates
    forkIO $ forever $ do
        threadDelay $ 500 * 1000
        modifyMVar_ curRev $ \cr -> do
            let crn = cr + 1
            -- send update to all nodes uptodate
            ns <- readIORef nodesRev
            let nodesUpToDate = Map.keys $ Map.filter (== cr) ns
            forM_ nodesUpToDate $ \i -> sendUpdate sock i crn
            return crn

    -- worker for distributing updates
    -- now loop and:
    --      o update nodes not on current revision
    --      o receive node responses and update revision list 
    forever $ do
        -- update revision list
        (ident, msg) <- receiveReply sock
        case CS.head msg of
            'S' -> withMVar curRev $ \cr -> do 
                   forM_ [0..cr] $ \rev -> do
                              sendUpdate sock ident rev
                              -- now also wait for it updating
                              (_, msgnr) <- receiveReply sock
                              when (msgToRev msgnr /= rev) $ error "revision update for new node failed"
                   addNode nodesRev ident cr
            'D' -> incRevNode nodesRev ident (msgToRev msg)
            _ -> error $ "unknown message: " ++ show msg
        CS.putStrLn $ CS.append (formatID ident) msg
        hFlush stdout
    -- cleanup zmq stuff
    unbind sock addr
    term ctx

formatID i = CS.cons '[' $ CS.append i "] "

addNode :: IORef (Map.Map CS.ByteString Int) -> CS.ByteString -> Int -> IO ()
addNode ns i rev = modifyIORef ns (Map.insert i rev)

incRevNode :: IORef (Map.Map CS.ByteString Int) -> CS.ByteString -> Int -> IO ()
incRevNode ns i r = modifyIORef ns (Map.adjust (const r) i)

receiveReply :: Receiver t => Socket t -> IO (CS.ByteString, CS.ByteString)
receiveReply sock = do
    ident <- receive sock 
    _ <- receive sock 
    msg <- receive sock 
    return (ident, msg)

sendUpdate :: Sender t => Socket t -> CS.ByteString -> Int -> IO ()
sendUpdate sock ident num = do
    send sock [SendMore] ident
    send sock [SendMore] ""
    send sock [] $ CS.cons 'U' (CS.pack (show (num :: Int)))

msgToRev :: CS.ByteString -> Int
msgToRev m = fst $ fromMaybe (0,"") (CS.readInt $ CS.tail m)

