d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), rw-sync_client.cc rw-sync_transaction.cc)

OBJ-rw-sync-transaction := $(LIB-store-frontend) $(o)rw-sync_transaction.o

OBJ-rw-sync-client := $(o)rw-sync_client.o

LIB-rw-sync := $(OBJ-rw-sync-client) $(OBJ-rw-sync-transaction) 
