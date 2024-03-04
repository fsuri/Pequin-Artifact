d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc)

PROTOS += $(addprefix $(d), toy-proto.proto)

OBJS-toy-client := $(LIB-message) $(LIB-udptransport) $(LIB-request) $(LIB-store-common) $(LIB-store-frontend) \
									$(o)toy-proto.o $(o)client.o 

LIB-toy-store := $(LIB-message) $(LIB-udptransport) $(LIB-request) \
									$(LIB-store-common) $(LIB-store-backend) \
									$(o)toy-proto.o $(o)server.o

