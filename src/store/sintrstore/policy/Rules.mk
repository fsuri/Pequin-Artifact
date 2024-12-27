d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), policy_client.cc policy_parse_client.cc weight_policy.cc acl_policy.cc)

PROTOS += $(addprefix $(d), policy-proto.proto)

LIB-sintr-policy := $(LIB-store-frontend) $(o)policy_client.o $(o)policy-proto.o \
	$(o)policy_parse_client.o $(o)weight_policy.o $(o)acl_policy.o
