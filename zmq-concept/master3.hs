-- this is mostly master2.hs, mixed with some of the original concept
--
-- another communication concept, using two sockets
--  o dealer-router (s-m) for slaves requests
--      slave connects, tells master where he is
--  o request-response (s-m) for actual state updates 
--      master connects back
--
--  reason to think about this concept is that zmq sockets are not thread-safe
--  therefore splitting the former single socket in multiple could be sensible
--
--  idea behind it is: it should be so simple
--
--  S: Hey, I'm at revision 23.
--  M: Here, have some updates till 123.
--  S: I'm at 123. More?
--  M: Here, have some updates till 127.
--  S: I'm at 127. More?
--    <silence, nothing happened>
--  M: Here, have 128.
--  S: I'm at 128. More?
--  ...
--  After each "More?" the Master either sends Updates that happened before
--  or waits for a Signal that a new Update is in progress/happened.
-- 
--  Random Requests by Slaves (UpdateRequest, SlaveQuit...) have to go over
--  another channel; especially also NewSlave, which needs forkIOing the
--  communication thread.

import System.ZMQ4

import Control.Monad (forever)
import Control.Concurrent.Chan
import Data.ByteString (ByteString)

type NodeID = Int
type NodeAddr = String
type ReqID = Int
type RepItem = ((Tagged ByteString), Either (ReqID) (NodeID,ReqID))


main :: IO ()
main = do
    -- bind router sock
    -- open local state
    forever $ do
        -- wait for requests to
        --  o connect a new node
        --      add node in node status
        --      forkio rephandler for that node
        --  o issue an update
        --      put it into replicationChan
        --      ATH: careful with majority serialization: 
        --          here or in replicationHandler?
        --  o quit
        --      kill rephandler
        --      remove from node status
        undefined

localRepHandler :: Chan RepItem -> IO ()
localRepHandler chan = forever $ do
    -- read from chan
    -- replicate locally
    undefined

-- there's one for each slave
--      threadids must be taken care of (handler no longer needed -> kill)
replicationHandler :: Chan RepItem -> NodeID -> NodeAddr -> IO ()
replicationHandler chan id addr = do
    -- connect
    -- send all old updates
    -- send crc
    -- clone replicationChan 
    forever $ do
        -- take update from chan
        -- send it to slave (if requested by a slave it's marked in there)
        -- wait slave done msg
        undefined
    
