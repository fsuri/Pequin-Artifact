d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc)

$(d)server: $(LIB-tapir-store) $(LIB-strong-store) $(LIB-weak-store) \
	$(LIB-udptransport) $(LIB-tcptransport) $(LIB-morty-store) $(o)server.o \
	$(LIB-janus-store) $(LIB-io-utils) $(LIB-store-common-stats) \
	$(LIB-indicus-store) $(LIB-pbft-store) $(LIB-hotstuff-store) $(LIB-augustus-store) \
	$(LIB-bftsmart-store) $(LIB-bftsmart-augustus-store) $(LIB-bftsmart-stable-store) \
	$(LIB-pequin-store) $(LIB-tpcc)

BINS += $(d)server
