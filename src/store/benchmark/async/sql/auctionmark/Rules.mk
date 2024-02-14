d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), auctionmark_client.cc auctionmark_generator.cc auctionmark_transaction.cc auctionmark_profile.cc)

# $(info $$d is [${d}])
# $(info $$SRCS is [${SRCS}])

SRCS += $(addprefix $(d), close_auctions.cc get_item.cc get_user_info.cc new_bid.cc new_comment_response.cc \
						 new_comment.cc new_feedback.cc new_item.cc new_purchase.cc update_item.cc )

LIB-auctionmark-profile := $(o)auctionmark_profile.o

OBJ-auctionmark-client := $(o)auctionmark_client.o

OBJ-auctionmark-transaction := $(LIB-store-frontend) $(LIB-auctionmark-utils) $(o)auctionmark_transaction.o

LIB-auctionmark-transactions := $(OBJ-auctionmark-transaction) \
					$(o)close_auctions.o $(o)get_item.o $(o)get_user_info.o $(o)new_bid.o \
					$(o)new_comment.o $(o)new_comment_response.o $(o)new_feedback.o $(o)new_item.o \
					$(o)new_purchase.o $(o)update_item.o

LIB-auctionmark :=  $(LIB-auctionmark-utils) $(LIB-auctionmark-profile)  $(LIB-auctionmark-transactions) $(OBJ-auctionmark-client) 


# $(d)auctionmark_generator: $(LIB-io-utils) $(o)auctionmark_generator.o $(LIB-auctionmark-utils) $(LIB-auctionmark-profile)

# BINS += $(d)auctionmark_generator

# cd := $(d)
# include $(cd)utils/Rules.mk