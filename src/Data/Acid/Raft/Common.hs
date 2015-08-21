module Data.Acid.Raft.Common
    where

import Data.ByteString (ByteString)
import Data.Serialize (Serialize(..), encode, decode,
                       put, get, putWord8, getWord8)
import Control.Monad (liftM, liftM2)

import System.Timeout (timeout)

----------------------------------------------------------------------
-- Node state related stuff.

-- | A node take one of three roles:
data NodeRole = Follower
              | Candidate
              | Leader

-- | Logical clock for raft.
type Term = Int

-- | ID of a log-entry.
--type EntryId = Int

-- | Each node has a unique identity.
type NodeIdentity = ByteString

-- | A set of all nodes known.
type NodeSet = [NodeIdentity]

-- | State of a node, that is permuted whilest conducting raft.
data NodeState = NodeState {
      nIdent        :: NodeIdentity -- ^ our id
    , nSet          :: NodeSet      -- ^ all node ids
    , nRole         :: NodeRole     -- ^ role we take
    , nTerm         :: Term         -- ^ our current term
    , nVotedFor     :: NodeIdentity -- ^ the node we last voted for
    , nVotesGranted :: Int          -- ^ number of votes a follower got
    , nTimeout      :: Int          -- ^ the current timeout in ms
    }

----------------------------------------------------------------------
-- Communication related stuff.

-- | Messages that occur in raft.
data Message = Timeout
             | VoteRequest Term NodeIdentity -- todo: EntryId Term
             | VoteResult Term Bool
             | Heartbeat Term

instance Serialize Message where
    put msg = case msg of
        VoteRequest t i     -> putWord8 0 >> put t >> put i
        VoteResult t g      -> putWord8 1 >> put t >> put g
        Heartbeat t         -> putWord8 2 >> put t
        Timeout             -> putWord8 9 >> error "Not serializable"
    get = do
        t <- getWord8
        case t of
            0 -> liftM2 VoteRequest get get
            1 -> liftM2 VoteResult get get
            2 -> liftM Heartbeat get
            _ -> error $
                 "Data.Serialize.get failed for Message with tag " ++ show t

-- | Messages are sent to one receiver or broadcasted.
data Envelope = Receiver NodeIdentity Message
              | Broadcast Message

-- | Receive messages via comm-chan or timeout.
receiveMsgs :: ComChan -> Int -> IO Message
receiveMsgs cc t = do
    mayMsg <- timeout t (ccReceive cc)
    case mayMsg of
        Nothing -> return Timeout
        Just m  -> either
          (\e -> error $ "Data.Serialize.decode failed on Message: " ++ show e)
          return (decode m)

-- | Send a message via comm-chan.
sendMsg :: ComChan -> NodeIdentity -> Message -> IO ()
sendMsg cc n = ccSendTo cc n . encode

-- | Broadcast a message via comm-chan.
-- Careful: Broadcasts must not include the sender!
broadcastMsg :: ComChan -> Message -> IO ()
broadcastMsg cc = ccBroadcast cc . encode

-- | Process an envelope (message with receiver)
processEnvelope :: ComChan -> Envelope -> IO ()
processEnvelope cc (Receiver r m) = sendMsg cc r m
processEnvelope cc (Broadcast m) = broadcastMsg cc m

-- | A communication channel.
data ComChan = ComChan {
      ccSendTo    :: ByteString -> ByteString -> IO ()
    , ccBroadcast :: ByteString -> IO ()
    , ccReceive :: IO ByteString
    }

