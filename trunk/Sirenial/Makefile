default:
	ghci -fwarn-unused-imports BuurtlinkQueries

configure:
	cabal configure

docs: configure
	cabal haddock

opendocs: docs
	open dist/doc/html/Sirenial/index.html
