all: build

deps:
	rebar3 do deps unlock, deps upgrade

build: deps
	rebar3 compile
	make jars

jars:
	_build/default/lib/erlmld/priv/download.sh

release: build
	rebar3 tar

clean:
	rebar3 clean
