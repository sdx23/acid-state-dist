This directory contains some examples demonstrating the use of distributed
acid-state.

The slave1/2 directories contain symlinks for quick testing on one machine.

# HelloWorld

A simple example analogous to the HelloWorld example in acid-state.
Run the Master directly here, Slaves in subdirectories.

The "safe" Slaves demonstrate how to take care of exceptions.

# Int*Interactive

Contains in Int as state, allows for multiple Updates in a row. The IntState is
used in the _tests_ as well.

# Threaded

Demonstrates how Exceptions are handled in threaded usages.
