#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_value_id.h"

namespace auctionmark {

const std::vector<int> GlobalAttributeValueId::COMPOSITE_BITS = { GlobalAttributeGroupId::ID_LENGTH, INT_MAX_DIGITS };

GlobalAttributeValueId::GlobalAttributeValueId(const std::string& group_attribute_id, int id)
    : group_attribute_id(group_attribute_id), id(id) {}

GlobalAttributeValueId::GlobalAttributeValueId(const GlobalAttributeGroupId& group_attribute_id, int id)
    : group_attribute_id(group_attribute_id.encode()), id(id) {}

std::string GlobalAttributeValueId::encode() const{
    return CompositeId::encode(COMPOSITE_BITS);
}

void GlobalAttributeValueId::decode(const std::string& composite_id) {
    std::vector<std::string> values = CompositeId::decode(composite_id, COMPOSITE_BITS);
    group_attribute_id = values[0];
    id = std::stoi(values[1]);
}

std::vector<std::string> GlobalAttributeValueId::to_vec() const{
    return {group_attribute_id, std::to_string(id)};
}

GlobalAttributeGroupId GlobalAttributeValueId::get_global_attribute_group() const {
    return GlobalAttributeGroupId(group_attribute_id);
}

int GlobalAttributeValueId::get_id() const {
    return id;
}

bool GlobalAttributeValueId::operator==(const GlobalAttributeValueId& other) const {
    return group_attribute_id == other.group_attribute_id && id == other.id;
}

bool GlobalAttributeValueId::operator<(const GlobalAttributeValueId& other) const {
    if(group_attribute_id != other.group_attribute_id){
        return group_attribute_id < other.group_attribute_id;
    }
    else{
        return id < other.id;
    } 
}

} // namespace auctionmark