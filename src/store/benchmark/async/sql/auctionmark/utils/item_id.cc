#include "store/benchmark/async/sql/auctionmark/utils/item_id.h"

namespace auctionmark {

const std::vector<int> ItemId::COMPOSITE_BITS = { UserId::ID_LENGTH, INT_MAX_DIGITS };

ItemId::ItemId(): seller_id(), item_ctr(0){}

ItemId::ItemId(const std::string& seller_id, int item_ctr)
    : seller_id(seller_id), item_ctr(item_ctr) {}

ItemId::ItemId(const UserId& seller_id, int item_ctr)
    : seller_id(seller_id), item_ctr(item_ctr) {}

ItemId::ItemId(const std::string& composite_id) {
    decode(composite_id);
}

ItemId::ItemId(const ItemId& other)  // copy constructor
        : seller_id(other.seller_id), item_ctr(other.item_ctr) {}

std::string ItemId::encode() const {
    return CompositeId::encode(COMPOSITE_BITS);
}

void ItemId::decode(const std::string& composite_id) {
    std::vector<std::string> values = CompositeId::decode(composite_id, COMPOSITE_BITS);
    seller_id = UserId(values[0]);
    item_ctr = std::stoi(values[1]);
}

std::vector<std::string> ItemId::to_vec() const {
    return {seller_id.encode(), std::to_string(item_ctr)};
}

UserId ItemId::get_seller_id() const {
    return seller_id;
}

int ItemId::get_item_ctr() const {
    return item_ctr;
}

std::string ItemId::to_string() const {
    std::ostringstream oss;
    oss << "ItemId<item_ctr=" << item_ctr << ", seller_id=" << seller_id.encode() << ", encoded=" << encode() << ">";
    return oss.str();
}

bool ItemId::operator==(const ItemId& other) const {
    return item_ctr == other.item_ctr && seller_id == other.seller_id;
}

bool ItemId::operator<(const ItemId& other) const {
    if (seller_id != other.seller_id) {
        return seller_id < other.seller_id;
    }
    return item_ctr < other.item_ctr;
}

} // namespace auctionmark