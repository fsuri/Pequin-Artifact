d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcc_client.cc tpcc_transaction.cc new_order.cc new_order_sequential.cc payment.cc payment_sequential.cc order_status.cc stock_level.cc \
		  delivery.cc delivery_sequential.cc tpcc_generator.cc tpcc_utils.cc) 

OBJ-sql-tpcc-transaction := $(LIB-store-frontend) $(o)tpcc_transaction.o

OBJ-sql-tpcc-client := $(o)tpcc_client.o

LIB-sql-tpcc := $(OBJ-sql-tpcc-client) $(OBJ-sql-tpcc-transaction) $(o)new_order.o $(o)new_order_sequential.o $(o)payment.o \
	$(o)payment_sequential.o $(o)order_status.o $(o)stock_level.o $(o)delivery.o $(o)delivery_sequential.o $(o)tpcc_utils.o

$(d)sql_tpcc_generator: $(LIB-io-utils) $(o)tpcc_generator.o $(o)tpcc_utils.o

BINS += $(d)sql_tpcc_generator

