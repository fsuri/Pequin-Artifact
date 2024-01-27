#include <string>
#include <optional>

class ItemCommentResponse {
private:
    std::optional<long> commentId;
    std::string itemId;
    std::string sellerId;

public:
    ItemCommentResponse(std::optional<long> commentId, std::string itemId, std::string sellerId)
        : commentId(commentId), itemId(itemId), sellerId(sellerId) {}

    std::optional<long> getCommentId() const {
        return this->commentId;
    }

    std::string getItemId() const {
        return this->itemId;
    }

    std::string getSellerId() const {
        return this->sellerId;
    }

    bool operator==(const ItemCommentResponse& other) const {
        return commentId == other.commentId
            && itemId == other.itemId
            && sellerId == other.sellerId;
    }
};