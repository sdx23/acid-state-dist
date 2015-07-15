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

type NodeID = Int
type NodeAddr = String


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
        --      serialize it on master
        --      set update-occured-event - slaves will replicate
        --      ATH: at this point we forbid majority serialization!
        --          we can't use a chan (as in the old concept) as this again 
        --          requires to split the socket between different threads
        --          instead we could use a chan holding events not serialized on
        --          master - this needs distinction by rev in the rephandler 
        --  o quit
        --      kill rephandler
        --      remove from node status
        undefined


-- there's one for each slave
--      threadids must be taken care of (handler no longer needed -> kill)
--      we need an update-occured-event
--      we also need a mechanism to ensure a slave knows it's an update he requested
--          ATH: this is not trivial - we can't get this info from the master log
replicationHandler :: NodeID -> NodeAddr -> IO ()
replicationHandler id addr = do
    -- connect
    forever $ do
        -- await slave-request for updates
        -- if curstate newer
            -- send state update
        -- else
            -- wait update-event, then send state update
        undefined
    
