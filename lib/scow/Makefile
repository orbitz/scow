CAMLP4=

OCAMLDEP_OPTS=-package core,async,gen_server
OCAMLC_OPTS=$(OCAMLDEP_OPTS) -w '@f@p@u@s@40'
OCAMLOPT_OPTS=$(OCAMLC_OPTS)

LIB_MODULES=scow.ml
NON_LIB_MODULES=

BYTE_TARGETS=scow.cma
NATIVE_TARGETS=scow.cmxa

.PHONY: all test

all: native-code byte-code

test:

# Dependencies
include ../Ocamlrules.mk.in
