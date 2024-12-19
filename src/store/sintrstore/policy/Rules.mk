d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), policy.cc weight_policy.cc acl_policy.cc)

LIB-sintr-policy := $(LIB-store-frontend) $(o)policy.o $(o)weight_policy.o $(o)acl_policy.o
