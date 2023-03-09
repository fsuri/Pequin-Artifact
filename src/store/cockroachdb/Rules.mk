d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc )

# TaoPQ static libraries
LIB-taopq :=

LIB-cockroachdb-store := $(o)common.o $(o)slots.o $(o)replica.o $(o)server.o \
	$(o)server-proto.o $(o)app.o $(o)shardclient.o \
	$(o)client.o $(LIB-crypto) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) 
