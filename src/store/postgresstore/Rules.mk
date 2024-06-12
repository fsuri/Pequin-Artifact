d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc app.cc)

LIB-postgres-client := $(LIB-store-frontend) $(LIB-store-common) $(o)client.o $(o)server.o $(o)app.o

