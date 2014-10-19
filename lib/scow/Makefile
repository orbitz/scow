CAMLP4=

OCAMLDEP_OPTS=-package core,async,gen_server,event_engine
OCAMLC_OPTS=$(OCAMLDEP_OPTS) -w '@f@p@u@s@40'
OCAMLOPT_OPTS=$(OCAMLC_OPTS)

TRANSPORTS=scow_transport_faulty.ml scow_transport_timeout.ml \
	scow_transport_party.ml

LIB_MODULES=scow_log_index.ml scow.ml scow_statem.ml \
	scow_transport.ml scow_log.ml scow_rpc.ml \
	scow_term.ml scow_store.ml scow_event_table.ml \
	scow_timer.ml scow_transport_memory.ml \
	scow_store_memory.ml scow_log_memory.ml \
	scow_notify.ml scow_replication_state.ml \
	scow_msg.ml scow_replication_msg.ml \
	$(TRANSPORTS)

NON_LIB_MODULES=

EXTERNAL_DEPS= \
	../event_engine/event_engine.cmxa \
	../event_engine/event_engine.cma

BYTE_TARGETS=scow.cma
NATIVE_TARGETS=scow.cmxa

.PHONY: all test install

all: native-code byte-code

install: all
	ocamlfind install scow META $(BYTE_TARGETS) $(NATIVE_TARGETS) scow.a *.cmx *.cmi *.mli

test:

# Dependencies
include ../Ocamlrules.mk.in
