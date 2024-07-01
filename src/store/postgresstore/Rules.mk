d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc app.cc)

LIB-postgres-client := $(LIB-store-frontend) $(LIB-store-common) $(o)client.o $(o)server.o $(o)app.o

LIB-postgresstore := $(o)server.o $(o)client.o \
	$(LIB-configuration) $(LIB-store-common) \
	$(LIB-transport)