d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc server_fallback.cc servertools.cc concurrencycontrol.cc store.cc common.cc \
		phase1validator.cc localbatchsigner.cc sharedbatchsigner.cc \
		basicverifier.cc localbatchverifier.cc sharedbatchverifier.cc \
		querysync-server.cc querysync-servertools.cc querysync-tests.cc querysync-client.cc queryexec.cc checkpointing.cc snapshot_mgr.cc sql_interpreter.cc \
		concurrencycontrol_semantic.cc validation_parse_client.cc client2client.cc endorsement_client.cc endorsement_policy.cc \
		validation_client.cc)

PROTOS += $(addprefix $(d), sintr-proto.proto)
PROTOS += $(addprefix $(d), query-proto.proto)

#LIB-sql_interpreter := $(o)sql_interpreter.o
LIB-sintr-common := $(LIB-store-backend-sql-encoding) $(o)common.o $(o)snapshot_mgr.o $(o)sql_interpreter.o


SRCS += $(addprefix $(d), table_store_interface_peloton.cc table_store_interface_toy.cc) # table_store_interface_old.cc)
#LIB-table-store-interface := $(o)table_store_interface_peloton.o $(o)table_store_interface_toy.o 
LIB-query-engine := $(LIB-binder) $(LIB-catalog) $(LIB-common) $(LIB-concurrency) $(LIB-executor) $(LIB-expression) $(LIB-function) \
	$(LIB-gc) $(LIB-index) $(LIB-murmur) $(LIB-optimizer) $(LIB-parser) $(LIB-planner) $(LIB-settings) $(LIB-storage) $(LIB-threadpool) $(LIB-traffic-cop) \
	$(LIB-type) $(LIB-trigger) $(LIB-util)


LIB-sintr-store := $(o)server.o $(o)server_fallback.o $(o)servertools.o $(o)querysync-server.o $(o)querysync-servertools.o $(o)querysync-tests.o \
	$(o)concurrencycontrol.o $(o)concurrencycontrol_semantic.o $(LIB-latency) \
	$(o)sintr-proto.o $(o)query-proto.o $(LIB-sintr-common) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-bft-tapir-config) \
	$(LIB-configuration) $(LIB-store-common) $(LIB-transport) $(o)phase1validator.o \
	$(o)localbatchsigner.o $(o)sharedbatchsigner.o $(o)basicverifier.o $(o)localbatchverifier.o $(o)sharedbatchverifier.o \
	$(LIB-query-engine) $(o)table_store_interface_peloton.o $(o)table_store_interface_toy.o \
	$(o)endorsement_policy.o  #$(o)table_store_interface_old.o

LIB-sintr-validation := $(LIB-store-frontend) $(LIB-validation-tpcc) $(o)validation_parse_client.o $(o)validation_client.o

LIB-sintr-client := $(LIB-udptransport) \
	$(LIB-store-frontend) $(LIB-store-common) $(o)sintr-proto.o $(o)query-proto.o\
	$(o)shardclient.o $(o)querysync-client.o $(o)client.o $(LIB-bft-tapir-config) \
	$(LIB-crypto) $(LIB-batched-sigs) $(LIB-sintr-common) $(o)phase1validator.o \
	$(o)basicverifier.o $(o)localbatchverifier.o $(LIB-sintr-validation) \
	$(o)client2client.o $(o)endorsement_client.o $(o)endorsement_policy.o


LIB-proto := $(o)sintr-proto.o $(o)query-proto.o

# $(o)../../query-engine/traffic_cop/traffic_cop.o
#-I/home/floriansuri/Research/Projects/Sintr/Sintr-Artifact/src/store/common

#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o
#$(d)tbb_test: $(o)tbb_test.o
#$(d)compression_test: $(o)compression_test.o

#BINS += $(d)proto_bench $(d)tbb_test $(d)compression_test

include $(d)tests/Rules.mk
