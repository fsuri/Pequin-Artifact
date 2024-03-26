d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc)

$(d)server: $(LIB-tapir-store) $(LIB-strong-store) $(LIB-weak-store) \
	$(LIB-udptransport) $(LIB-tcptransport) $(LIB-morty-store) $(o)server.o \
	$(LIB-janus-store) $(LIB-io-utils) $(LIB-store-common) $(LIB-store-common-stats) \
	$(LIB-pbft-store) $(LIB-hotstuff-store) $(LIB-hotstuff-pg-store) $(LIB-augustus-store) \
	$(LIB-bftsmart-store) $(LIB-bftsmart-augustus-store) $(LIB-bftsmart-stable-store) \
	$(LIB-tpcc) $(LIB-indicus-store) $(LIB-pequin-store) $(LIB-postgres-store) \
	$(LIB-peloton-store) $(LIB-cockroachdb-store)

BINS += $(d)server
