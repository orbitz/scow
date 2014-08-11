CAMLP4=

OCAMLDEP_OPTS=-package core,async,gen_server
OCAMLC_OPTS=$(OCAMLDEP_OPTS) -w '@f@p@u@s@40'
OCAMLOPT_OPTS=$(OCAMLC_OPTS)

LIB_MODULES=scow_log_index.ml scow.ml scow_statem.ml \
	scow_transport.ml scow_log.ml scow_rpc.ml \
	scow_term.ml scow_store.ml scow_server_state.ml \
	scow_server_msg.ml scow_server_candidate.ml \
	scow_server_follower.ml scow_server_leader.ml \
	scow_timer.ml scow_transport_memory.ml \
	scow_store_memory.ml scow_log_memory.ml \
	scow_transport_faulty.ml scow_transport_timeout.ml
NON_LIB_MODULES=

BYTE_TARGETS=scow.cma
NATIVE_TARGETS=scow.cmxa

.PHONY: all test

all: native-code byte-code

test:

# Dependencies
include ../Ocamlrules.mk.in
