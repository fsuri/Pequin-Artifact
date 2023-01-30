d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), SQLParser.cc SQLParserResult.cc sql/Expr.cc sql/CreateStatement.cc sql/PrepareStatement.cc sql/SQLStatement.cc sql/statements.cc util/sqlhelper.cc parser/bison_parser.cc parser/flex_lexer.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

LIB-queryprocess := $(o)SQLParserResult.o $(o)sql/Expr.o $(o)sql/SQLStatement.o $(o)sql/statements.o $(o)sql/CreateStatement.o $(o)sql/PrepareStatement.o $(o)util/sqlhelper.o
LIB-queryprocess-flex := $(o)parser/flex_lexer.o


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)proto_bench

#include $(d)parser/Rules.mk
#include $(d)sql/Rules.mk
#include $(d)util/Rules.mk

