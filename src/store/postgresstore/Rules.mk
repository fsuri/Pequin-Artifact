d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc)

LIB-postgres-client := $(LIB-store-frontend) $(LIB-store-common) $(o)client.o 

LIB-postgres-store := $(o)server.o $(LIB-configuration) $(LIB-store-common) $(LIB-transport)

