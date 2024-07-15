d := $(dir $(lastword $(MAKEFILE_LIST)))

#parallel version
#SRCS += $(addprefix $(d), tpcc_client.cc tpcc_transaction.cc new_order.cc payment.cc order_status.cc stock_level.cc delivery.cc tpcc_generator.cc tpcc_utils.cc) 

#sequential version
 SRCS += $(addprefix $(d), tpcc_client.cc tpcc_transaction.cc new_order_sequential.cc payment_sequential.cc order_status.cc stock_level.cc delivery_sequential.cc tpcc_generator.cc tpcc_utils.cc) 

OBJ-sql-tpcc-transaction := $(LIB-store-frontend) $(o)tpcc_transaction.o

OBJ-sql-tpcc-client := $(o)tpcc_client.o

#parallel version
#LIB-sql-tpcc := $(OBJ-sql-tpcc-client) $(OBJ-sql-tpcc-transaction) $(o)new_order.o \
	$(o)payment.o $(o)order_status.o $(o)stock_level.o $(o)delivery.o $(o)tpcc_utils.o

#sequential version
LIB-sql-tpcc := $(OBJ-sql-tpcc-client) $(OBJ-sql-tpcc-transaction) $(o)new_order_sequential.o \
	$(o)payment_sequential.o $(o)order_status.o $(o)stock_level.o $(o)delivery_sequential.o $(o)tpcc_utils.o

$(d)sql_tpcc_generator: $(LIB-io-utils) $(o)tpcc_generator.o $(o)tpcc_utils.o

BINS += $(d)sql_tpcc_generator