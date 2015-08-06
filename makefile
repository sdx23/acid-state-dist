test:
	cabal clean
	cabal configure --enable-test
	cabal build
	cabal test | grep --color -C 999 PASS

bench:
	cabal clean
	cabal configure --enable-benchmarks -f-debug
	cabal build
	cabal bench

clean-all-state:
	find . -name state -type d -exec rm -rf {} \;
