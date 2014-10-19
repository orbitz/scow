CAMLP4=

OCAMLDEP_OPTS=-package core,async
OCAMLC_OPTS=$(OCAMLDEP_OPTS) -w '@f@p@u@s@40'
OCAMLOPT_OPTS=$(OCAMLC_OPTS)

LIB_MODULES=event_engine.ml
NON_LIB_MODULES=

BYTE_TARGETS=event_engine.cma
NATIVE_TARGETS=event_engine.cmxa

.PHONY: all test install

all: native-code byte-code

# install: all
# 	ocamlfind install event_engine META $(BYTE_TARGETS) $(NATIVE_TARGETS) event_engine.a *.cmx *.cmi *.mli

test:

# Dependencies
include ../Ocamlrules.mk.in
