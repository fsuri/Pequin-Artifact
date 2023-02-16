d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), promise.cc timestamp.cc tracer.cc \
				transaction.cc truetime.cc stats.cc partitioner.cc \
        pinginitiator.cc query_result_proto_wrapper_field.cc query_result_proto_wrapper_row.cc \
		query_result_proto_wrapper.cc)

PROTOS += $(addprefix $(d), common-proto.proto query-result-proto.proto)

LIB-store-common-stats := $(o)stats.o

LIB-store-common := $(LIB-message) $(o)common-proto.o $(o)promise.o \
		$(o)timestamp.o $(o)tracer.o $(o)transaction.o $(o)truetime.o \
		$(LIB-store-common-stats) $(o)partitioner.o $(o)pinginitiator.o \
		$(o)query-result-proto.o $(o)query_result_proto_wrapper.o $(o)query_result_proto_wrapper_row.o \
		$(o)query_result_proto_wrapper_field.o

include $(d)backend/Rules.mk $(d)frontend/Rules.mk
