d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), rw-base_transaction.cc)

PROTOS += $(addprefix $(d), rw-validation-proto.proto)

OBJ-rw-base-transaction := $(LIB-store-frontend) $(o)rw-base_transaction.o

LIB-rw-base := $(OBJ-rw-base-transaction) $(o)rw-validation-proto.o

cd := $(d)
include $(cd)sync/Rules.mk
include $(cd)validation/Rules.mk
