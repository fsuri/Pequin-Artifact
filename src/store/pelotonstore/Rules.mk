d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), app.cc replica.cc common.cc server.cc shardclient.cc client.cc testreplica.cc testclient.cc pbft_batched_sigs.cc bftsmartagent.cc table_store.cc)

PROTOS += $(addprefix $(d), pbft-proto.proto server-proto.proto)

# HotStuff static libraries
LIB-hotstuff-peloton-interface := store/hotstuffstore/libhotstuff/examples/libindicus_interface.a store/hotstuffstore/libhotstuff/salticidae/libsalticidae.a store/hotstuffstore/libhotstuff/libhotstuff.a store/hotstuffstore/libhotstuff/secp256k1/.libs/libsecp256k1.a

LIB-peloton-batched-sigs := $(LIB-crypto) $(o)pbft_batched_sigs.o 

LIB-peloton := $(LIB-adr-p) $(LIB-binder-p) $(LIB-catalog-p) $(LIB-common-p) $(LIB-concurrency-p) $(LIB-executor-p) $(LIB-expression-p) $(LIB-function-p) \
	$(LIB-gc-p) $(LIB-index-p) $(LIB-murmur-p) $(LIB-optimizer-p) $(LIB-parser-p) $(LIB-planner-p) $(LIB-settings-p) $(LIB-storage-p) $(LIB-threadpool-p) $(LIB-traffic-cop-p) \
	$(LIB-type-p) $(LIB-trigger-p) $(LIB-util-p) $(LIB-statistics-p) $(LIB-trigger-p) $(LIB-tuning-p) #$(LIB-udf-p)

# LIB-query-engine := $(LIB-binder) $(LIB-catalog) $(LIB-common) $(LIB-concurrency) $(LIB-executor) $(LIB-expression) $(LIB-function) \
	$(LIB-gc) $(LIB-index) $(LIB-murmur) $(LIB-optimizer) $(LIB-parser) $(LIB-planner) $(LIB-settings) $(LIB-storage) $(LIB-threadpool) $(LIB-traffic-cop) \
	$(LIB-type) $(LIB-trigger) $(LIB-util)


LIB-peloton-store := $(LIB-peloton) $(o)common.o $(o)replica.o $(o)server.o $(o)table_store.o\
	$(o)pbft-proto.o $(o)server-proto.o $(o)app.o $(o)bftsmartagent.o $(o)shardclient.o \
	$(o)client.o $(LIB-crypto) $(LIB-peloton-batched-sigs) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) $(LIB-store-backend) $(LIB-hotstuff-peloton-interface) $(LIB-peloton)