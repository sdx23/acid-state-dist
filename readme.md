# acid-state-dist

A replication backend for
[acid-state](http://github.com/acid-state/acid-state "acid-state").

Run an acid-state application on multiple nodes and have all instances operate
on the same state.
Two operation modes are provided:

	- Regular operation: without redundancy guarantees but fast,
	- Redundant operation: guarantees replication on _n_ nodes but slower.

Extented documentation is available via Haddock (/Hackage, as soon as released).

For questions and feedback, feel free to contact me by e-mail
<max.voit+hdv@with-eyes.net> or via IRC (sdx23 on irc.freenode.net).

