d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), rw-val_transaction.cc)

LIB-rw-val := $(LIB-rw-base) $(OBJ-rw-val-transaction) $(o)rw-val_transaction.o
