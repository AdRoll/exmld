.PHONY: docs

all: build

build:
	mix compile

docs: build
	@# run `mix escript.install hex ex_doc` first
	ex_doc "exmld" "git" _build/dev/lib/exmld/ebin
