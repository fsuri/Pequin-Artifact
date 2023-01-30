d := $(dir $(lastword $(MAKEFILE_LIST)))

#SRCS += $(addprefix $(d), bison_parser.cc flex_lexer.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

#LIB-queryprocess-parser := $(o)flex_lexer.o #$(o)bison_parser.o \
	$(o)flex_lexer.o #$(LIB-queryprocess-sql)


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)bison_parser + $(d)flex_lexer

#include $(d)tests/Rules.mk

