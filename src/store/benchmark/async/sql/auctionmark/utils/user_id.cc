#include "user_id.h"

namespace auctionmark {

const std::vector<int> UserId::COMPOSITE_BITS = { INT_MAX_DIGITS, INT_MAX_DIGITS };
const int UserId::ID_LENGTH = INT_MAX_DIGITS * 2;

UserId::UserId(int item_count, int offset) : item_count(item_count), offset(offset) {
}

UserId::UserId(std::string composite_id) {
    decode(composite_id);
}

UserId::UserId(const UserId& other) : item_count(other.item_count), offset(other.offset) {
}

UserId::UserId() : item_count(0), offset(0) {
}

std::string UserId::encode() const {
    return CompositeId::encode(COMPOSITE_BITS);
}

void UserId::decode(const std::string& composite_id) {
    std::vector<std::string> decoded_values = CompositeId::decode(composite_id, COMPOSITE_BITS);
    item_count = std::stoi(decoded_values[0]);
    offset = std::stoi(decoded_values[1]);
}

std::vector<std::string> UserId::to_vec() const {
    return {std::to_string(item_count), std::to_string(offset)};
}

int UserId::get_item_count() const {
    return item_count;
}

int UserId::get_offset() const {
    return offset;
}

std::string UserId::to_string() const {
    std::stringstream ss;
    ss << "UserId<item_count=" << item_count << ", offset=" << offset << ", encoded=" << encode() << ">";
    return ss.str();
}

bool UserId::operator==(const UserId &other) const {
    return item_count == other.item_count && offset == other.offset;
}

bool UserId::operator!=(const UserId &other) const {
    return !(*this == other);
}

bool UserId::operator<(const UserId &other) const {
    if (item_count != other.item_count) {
        return item_count < other.item_count;
    }
    return offset < other.offset;
}


size_t UserId::HashFunction::operator()(const UserId &point) const;
} // namespace auctionmark