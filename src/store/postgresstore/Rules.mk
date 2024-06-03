d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc client.cc )

LIB-postgresstore := $(o)server.o $(o)client.o \
	$(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport)