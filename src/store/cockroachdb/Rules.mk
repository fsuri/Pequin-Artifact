d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc client.cc )

LIB-cockroachdb-store := $(o)server.o $(o)client.o \
	$(LIB-crypto) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) \
	

