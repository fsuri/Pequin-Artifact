d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc servertools.cc concurrencycontrol.cc store.cc common.cc \
		phase1validator.cc localbatchsigner.cc sharedbatchsigner.cc \
		basicverifier.cc localbatchverifier.cc sharedbatchverifier.cc proto_bench.cc \
		querysync-server.cc querysync-client.cc queryexec.cc checkpointing.cc)

PROTOS += $(addprefix $(d), pequin-proto.proto)
PROTOS += $(addprefix $(d), query-proto.proto)

LIB-pequin-store := $(o)server.o $(o)servertools.o $(o)querysync-server.o $(o)concurrencycontrol.o $(LIB-queryprocess) $(LIB-latency) \
	$(o)pequin-proto.o $(o)query-proto.o $(o)common.o $(LIB-crypto) $(LIB-batched-sigs) $(LIB-bft-tapir-config) \
	$(LIB-configuration) $(LIB-store-common) $(LIB-transport) $(o)phase1validator.o \
	$(o)localbatchsigner.o $(o)sharedbatchsigner.o $(o)basicverifier.o \
	$(o)localbatchverifier.o $(o)sharedbatchverifier.o

LIB-pequin-client := $(LIB-udptransport) \
	$(LIB-store-frontend) $(LIB-store-common) $(o)pequin-proto.o $(o)query-proto.o\
	$(o)shardclient.o $(o)querysync-client.o $(o)client.o $(LIB-bft-tapir-config) \
	$(LIB-crypto) $(LIB-batched-sigs) $(o)common.o $(o)phase1validator.o \
	$(o)basicverifier.o $(o)localbatchverifier.o


LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

BINS += $(d)proto_bench

include $(d)tests/Rules.mk
