#ifndef AUCTIONMARK_ITEM_COMMENT_RESPONSE_H
#define AUCTIONMARK_ITEM_COMMENT_RESPONSE_H

#include <string>
#include <optional>

namespace auctionmark {

class ItemCommentResponse {
private:
    long comment_id;
    std::string item_id;
    std::string seller_id;

public:
    inline ItemCommentResponse(long comment_id, std::string item_id, std::string seller_id)
        : comment_id(comment_id), item_id(item_id), seller_id(seller_id) {}

    inline long get_comment_id() const {
        return this->comment_id;
    }

    inline std::string get_item_id() const {
        return this->item_id;
    }

    inline std::string get_seller_id() const {
        return this->seller_id;
    }

    inline bool operator==(const ItemCommentResponse& other) const {
        return comment_id == other.comment_id
            && item_id == other.item_id
            && seller_id == other.seller_id;
    }
};

} // namespace auctionmark

#endif // AUCTIONMARK_ITEM_COMMENT_RESPONSE_H
