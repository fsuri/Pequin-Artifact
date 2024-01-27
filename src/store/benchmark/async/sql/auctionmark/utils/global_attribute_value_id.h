#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include "composite_id.h"
#include "global_attribute_group_id.h"

class GlobalAttributeValueId : CompositeId {
private:
    static const std::array<int, 2> COMPOSITE_BITS;

    std::string group_attribute_id;
    int id;

public:
    GlobalAttributeValueId(const std::string& group_attribute_id, int id)
        : group_attribute_id(group_attribute_id), id(id) {}

    GlobalAttributeValueId(const GlobalAttributeGroupId& group_attribute_id, int id)
        : group_attribute_id(group_attribute_id.encode()), id(id) {}

    std::string encode() {
        return encode(COMPOSITE_BITS);
    }

    void decode(const std::string& composite_id) {
        std::array<std::string, 2> values = decode(composite_id, COMPOSITE_BITS);
        group_attribute_id = values[0];
        id = std::stoi(values[1]);
    }

    std::array<std::string, 2> toArray() {
        return {group_attribute_id, std::to_string(id)};
    }

    GlobalAttributeGroupId getGlobalAttributeGroup() const {
        return GlobalAttributeGroupId(group_attribute_id);
    }

    int getId() const {
        return id;
    }

    bool operator==(const GlobalAttributeValueId& other) const {
        return group_attribute_id == other.group_attribute_id && id == other.id;
    }

    // Note: encode and decode functions are not provided as they are not straightforward in C++ as in Java
};
