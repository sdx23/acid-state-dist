# This makefile is only for convenience in development,
# use cabal for building.
all:
	echo "Not intended for building, only convenience functions."

all-test:
	cabal clean
	cabal configure --enable-test
	cabal build
	cabal test | grep --color -C 999 PASS

bench:
	cabal clean
	cabal configure --enable-benchmarks -f-debug
	cabal build
	[ -d benchresults ] || mkdir benchresults
	cabal bench | tee -a benchresults/`git rev-parse HEAD`-`date -I`.bench

clean-all-state:
	find . -name state -type d -exec rm -rf {} \;
