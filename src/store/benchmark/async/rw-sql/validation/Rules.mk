d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), rw-sql_val_transaction.cc)

LIB-rw-sql-val := $(LIB-rw-sql-base) $(OBJ-rw-sql-val-transaction) $(o)rw-sql_val_transaction.o