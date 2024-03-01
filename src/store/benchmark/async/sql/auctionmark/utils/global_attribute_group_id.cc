#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_group_id.h"

namespace auctionmark {

const std::vector<int> GlobalAttributeGroupId::COMPOSITE_BITS = { INT_MAX_DIGITS, INT_MAX_DIGITS, INT_MAX_DIGITS };
const int GlobalAttributeGroupId::ID_LENGTH = INT_MAX_DIGITS * 3;

GlobalAttributeGroupId::GlobalAttributeGroupId(int category_id, int id, int count)
    : category_id(category_id), id(id), count(count) {}

GlobalAttributeGroupId::GlobalAttributeGroupId(const std::string& composite_id) {
    decode(composite_id);
}

std::string GlobalAttributeGroupId::encode() const {
    return CompositeId::encode(COMPOSITE_BITS);
}

void GlobalAttributeGroupId::decode(const std::string& composite_id) {
    std::vector<std::string> values = CompositeId::decode(composite_id, COMPOSITE_BITS);
    category_id = std::stoi(values[0]);
    id = std::stoi(values[1]);
    count = std::stoi(values[2]);
}

std::vector<std::string> GlobalAttributeGroupId::to_vec() const{
    return {std::to_string(category_id), std::to_string(id), std::to_string(count)};
}

int GlobalAttributeGroupId::get_category_id() const {
    return category_id;
}

int GlobalAttributeGroupId::get_id() const {
    return id;
}

int GlobalAttributeGroupId::get_count() const {
    return count;
}

bool GlobalAttributeGroupId::operator==(const GlobalAttributeGroupId& other) const {
    return category_id == other.category_id && id == other.id && count == other.count;
}

} // namespace auctionmark