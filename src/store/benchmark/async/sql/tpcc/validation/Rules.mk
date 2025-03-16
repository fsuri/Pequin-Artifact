d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcc_transaction.cc new_order.cc new_order_sequential.cc payment.cc payment_sequential.cc order_status.cc stock_level.cc \
		  delivery.cc delivery_sequential.cc) 

OBJ-validation-sql-tpcc-transaction := $(LIB-store-frontend) $(o)tpcc_transaction.o

LIB-validation-sql-tpcc := $(OBJ-validation-sql-tpcc-transaction) $(o)new_order.o $(o)new_order_sequential.o $(o)payment.o \
	$(o)payment_sequential.o $(o)order_status.o $(o)stock_level.o $(o)delivery.o $(o)delivery_sequential.o
