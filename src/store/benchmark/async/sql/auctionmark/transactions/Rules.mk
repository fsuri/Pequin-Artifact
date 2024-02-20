d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), auctionmark_transaction.cc close_auctions.cc get_item.cc get_user_info.cc new_bid.cc new_comment_response.cc \
						 new_comment.cc new_feedback.cc new_item.cc new_purchase.cc update_item.cc )

OBJ-auctionmark-transaction := $(LIB-store-frontend) $(LIB-auctionmark-utils) $(o)auctionmark_transaction.o

LIB-auctionmark-transactions := $(OBJ-auctionmark-transaction) $(o)close_auctions.o $(o)get_item.o $(o)get_user_info.o $(o)new_bid.o \
					$(o)new_comment.o $(o)new_comment_response.o $(o)new_feedback.o $(o)new_item.o \
					$(o)new_purchase.o $(o)update_item.o
