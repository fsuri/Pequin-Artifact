d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc )

# TaoPQ static libraries
LIB-taopq :=

LIB-cockroachdb-store := $(o)server.o \
	$(LIB-crypto) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) \
	# $(o)client.o 

