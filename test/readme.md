This directory contains test cases to verify functionality.

# Simple

A test for simple replication. Change initial state by master, check whether
state replicated to Slave.

# SlaveUpdates

Can Slaves request Updates successfully?
Do they also get the transaction result (not only updated state)?
Do Master and other Slaves get the update?

# CRCFail

For diverged state the CRC check must fail (unless Checkpoints were replicated).


