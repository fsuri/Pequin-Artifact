d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), promise.cc timestamp.cc tracer.cc \
				transaction.cc truetime.cc stats.cc table_kv_encoder.cc partitioner.cc \
        pinginitiator.cc)

PROTOS += $(addprefix $(d), common-proto.proto)

LIB-store-common-stats := $(o)stats.o

LIB-store-backend-sql-encoding:= $(o)table_kv_encoder.o

LIB-store-common := $(LIB-message) $(o)common-proto.o $(o)promise.o \
		$(o)timestamp.o $(o)tracer.o $(o)transaction.o $(o)truetime.o \
		$(LIB-store-common-stats) $(LIB-store-backend-sql-encoding) $(o)partitioner.o $(o)pinginitiator.o 

include $(d)backend/Rules.mk $(d)frontend/Rules.mk $(d)query_result/Rules.mk



