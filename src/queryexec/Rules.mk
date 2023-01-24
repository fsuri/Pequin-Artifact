d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), block_handle.cc block_info.cc buffer_manager.cc catalog_manager.cc file_handle.cc file_info.cc \
		index_manager.cc interpreter.cc minidb_api.cc \
		record_manager.cc sql_statement.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

LIB-queryprocess := $(o)catalog_manager.o \
	$(o)interpreter.o $(o)minidb_api.o $(o)sql_statement.o 


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)proto_bench

#include $(d)tests/Rules.mk

