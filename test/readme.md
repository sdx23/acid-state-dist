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

# CheckpointSync

A diverged state must be the same after sync-replicating a Checkpoint (i.e.
Slave joins only after generating the checkpoint).

# OrderingRandom

It is essential to keep ordering of events identical on all nodes. This test
applies a non-commutative operation on the state to check this behaviour.

# NReplication

Test for the redundant operation mode. An Update shall only be accepted as soon
as enough Slaves joined.

# UpdateError

Updates containing 'error's are should fail when being scheduled.

# SyncTimeout

If there is no Master, synchronization is to time out.
