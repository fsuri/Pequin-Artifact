d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc )

LIB-blackhole-client := $(o)client.o $(LIB-crypto) $(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport) $(LIB-store-backend) $(LIB-hotstuff-interface)
