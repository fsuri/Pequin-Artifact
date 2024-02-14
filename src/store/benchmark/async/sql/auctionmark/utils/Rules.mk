d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), composite_id.cc flat_histogram.cc global_attribute_group_id.cc global_attribute_value_id.cc item_id.cc item_info.cc user_id.cc user_id_generator.cc zipf.cc category_parser.cc auctionmark_utils.cc)

LIB-auctionmark-utils := $(o)composite_id.o $(o)global_attribute_group_id.o \
	$(o)global_attribute_value_id.o $(o)item_id.o $(o)item_info.o $(o)user_id_generator.o \
	$(o)user_id.o $(o)zipf.o $(o)category_parser.o $(o)auctionmark_utils.o $(o)flat_histogram.o
