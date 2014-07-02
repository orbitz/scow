CAMLP4=

OCAMLDEP_OPTS=-package core,async
OCAMLC_OPTS=$(OCAMLDEP_OPTS)  -w '@f@p@u@s@40'
OCAMLOPT_OPTS=$(OCAMLC_OPTS)

LIB_MODULES=scow_raft.ml
NON_LIB_MODULES=scow.ml

BYTE_TARGETS=scow.cma
NATIVE_TARGETS=scow.cmxa

.PHONY: all test

all: native-code byte-code

test:

# Dependencies
include ../Ocamlrules.mk.in
