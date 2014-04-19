CAMLP4=

OCAMLDEP_OPTS= -package core
OCAMLC_OPTS=$(OCAMLDEP_OPTS)
OCAMLOPT_OPTS=$(OCAMLDEP_OPTS)

LIB_MODULES=scow_raft.ml
NON_LIB_MODULES=scow.ml

BYTE_TARGETS=scow.cma
NATIVE_TARGETS=scow.cmxa

.PHONY: all test

all: native-code byte-code

test:

# Dependencies
include ../Ocamlrules.mk.in
