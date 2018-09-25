.PHONY: deps

all: build

deps:
	mix do deps.unlock --all, deps.get

build: deps
	mix compile
	make jars

jars:
	./_build/dev/lib/erlmld/priv/download.sh

clean:
	mix clean
