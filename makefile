all-test:
	cabal clean
	cabal configure --enable-test
	cabal build
	cabal test | grep --color -C 999 PASS

clean-all-state:
	find . -name state -type d -exec rm -rf {} \;
