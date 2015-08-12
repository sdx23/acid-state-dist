#!/bin/bash

# start Slave
xterm -hold -e runhaskell ConLossSlave.hs &
# start Master
xterm -hold -e runhaskell ConLossMaster.hs &

# wait & kill connection
sleep 10
echo "impeding connection now"
sudo iptables -A INPUT -p tcp --dport 3333 -j DROP
sudo iptables -A INPUT -p tcp --sport 3333 -j DROP
# wait & restore connection
sleep 5
echo "stop impeding connection"
sudo iptables -D INPUT -p tcp --dport 3333 -j DROP
sudo iptables -D INPUT -p tcp --sport 3333 -j DROP

