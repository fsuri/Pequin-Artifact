d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), pingserver.cc \
				kvstore.cc lockserver.cc txnstore.cc versionstore.cc versionstore_safe.cc \
				versionstore_generic_safe.cc)

LIB-store-backend := $(o)kvstore.o $(o)lockserver.o $(o)txnstore.o $(o)versionstore.o $(o)versionstore_safe.o \
	$(o)pingserver.o $(o)versionstore_generic_safe.o

include $(d)tests/Rules.mk
#include $(d)tests/Rules.mk $(d)sql_engine/Rules.mk
