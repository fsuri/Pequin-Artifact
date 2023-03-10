d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcc_client.cc tpcc_transaction.cc new_order.cc payment.cc order_status.cc stock_level.cc delivery.cc)

OBJ-sql-tpcc-transaction := $(LIB-store-frontend) $(o)tpcc_transaction.o

OBJ-sql-tpcc-client := $(o)tpcc_client.o

LIB-sql-tpcc := $(OBJ-sql-tpcc-client) $(OBJ-sql-tpcc-transaction) $(o)new_order.o \
	$(o)payment.o $(o)order_status.o $(o)stock_level.o $(o)delivery.o
