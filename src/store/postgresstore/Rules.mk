d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc)

LIB-postgres-client := $(LIB-store-frontend) $(LIB-store-common) $(o)client.o

