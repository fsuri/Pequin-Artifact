d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc servertools.cc concurrencycontrol.cc store.cc common.cc \
		phase1validator.cc localbatchsigner.cc sharedbatchsigner.cc \
		basicverifier.cc localbatchverifier.cc sharedbatchverifier.cc proto_bench.cc \
		querysync-server.cc querysync-client.cc queryexec.cc checkpointing.cc tbb_test.cc compression_test.cc) # snapshot_mgr.cc)

PROTOS += $(addprefix $(d), pequin-proto.proto)
PROTOS += $(addprefix $(d), query-proto.proto)

LIB-pequin-store := $(o)server.o $(o)servertools.o $(o)querysync-server.o $(o)concurrencycontrol.o $(LIB-latency) \
	$(o)pequin-proto.o $(o)query-proto.o $(o)common.o $(LIB-crypto) $(LIB-batched-sigs) $(LIB-bft-tapir-config) \ #$(o)snapshot_mgr.o
	$(LIB-configuration) $(LIB-store-common) $(LIB-transport) $(o)phase1validator.o \
	$(o)localbatchsigner.o $(o)sharedbatchsigner.o $(o)basicverifier.o \
	$(o)localbatchverifier.o $(o)sharedbatchverifier.o

LIB-pequin-client := $(LIB-udptransport) \
	$(LIB-store-frontend) $(LIB-store-common) $(o)pequin-proto.o $(o)query-proto.o\
	$(o)shardclient.o $(o)querysync-client.o $(o)client.o $(LIB-bft-tapir-config) \
	$(LIB-crypto) $(LIB-batched-sigs) $(o)common.o $(o)phase1validator.o \ #$(o)snapshot_mgr.o
	$(o)basicverifier.o $(o)localbatchverifier.o


LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

$(d)tbb_test: $(o)tbb_test.o
$(d)compression_test: $(o)compression_test.o

BINS += $(d)proto_bench $(d)tbb_test $(d)compression_test

include $(d)tests/Rules.mk
