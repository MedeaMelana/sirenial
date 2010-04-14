default:
	ghci -Wall -fno-warn-missing-signatures -fno-warn-name-shadowing BuurtlinkQueries

configure:
	cabal configure

docs: configure
	cabal haddock

opendocs: docs
	open dist/doc/html/Sirenial/index.html

clean:
	rm -rf dist BuurtlinkTest
	find . -name '*.hi' -exec rm {} \;
	find . -name '*.o' -exec rm {} \;
