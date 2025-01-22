d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcc_transaction.cc new_order.cc payment.cc order_status.cc stock_level.cc delivery.cc policy_change.cc)

OBJ-validation-tpcc-transaction := $(LIB-store-frontend) $(o)tpcc_transaction.o

LIB-validation-tpcc := $(OBJ-validation-tpcc-transaction) $(LIB-tpcc) $(o)new_order.o \
	$(o)payment.o $(o)order_status.o $(o)stock_level.o $(o)delivery.o $(o)policy_change.o
