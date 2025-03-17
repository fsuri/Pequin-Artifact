d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), rw-sql_base_transaction.cc)

# PROTOS += $(addprefix $(d), rw-sql-validation-proto.proto)

OBJ-rw-sql-base-transaction := $(LIB-store-frontend) $(o)rw-sql_base_transaction.o

LIB-rw-sql-base := $(OBJ-rw-sql-base-transaction) #$(o)rw-sql-validation-proto.o

cd := $(d)
include $(cd)sync/Rules.mk
# include $(cd)validation/Rules.mk
