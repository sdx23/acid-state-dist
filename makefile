all-test:
	cabal clean
	cabal configure --enable-test
	cabal build
	cabal test

clean-all-state:
	find . -name state -type d -exec rm -rf {} \;
