#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include "composite_id.h"

class GlobalAttributeGroupId : CompositeId {
private:
    static const int COMPOSITE_BITS[3];

    int category_id;
    int id;
    int count;

public:
    GlobalAttributeGroupId(int category_id, int id, int count)
        : category_id(category_id), id(id), count(count) {}

    GlobalAttributeGroupId(const std::string& composite_id) {
        decode(composite_id);
    }

    std::string encode() {
        return encode(COMPOSITE_BITS);
    }

    void decode(const std::string& composite_id) {
        std::array<std::string, 3> values = decode(composite_id, COMPOSITE_BITS);
        category_id = std::stoi(values[0]);
        id = std::stoi(values[1]);
        count = std::stoi(values[2]);
    }

    std::array<std::string, 3> toArray() {
        return {std::to_string(category_id), std::to_string(id), std::to_string(count)};
    }

    int getCategoryId() const {
        return category_id;
    }

    int getId() const {
        return id;
    }

    int getCount() const {
        return count;
    }

    bool operator==(const GlobalAttributeGroupId& other) const {
        return category_id == other.category_id && id == other.id && count == other.count;
    }

    // Note: encode and decode functions are not provided as they are not straightforward in C++ as in Java
};

const std::array<int, 3> GlobalAttributeGroupId::COMPOSITE_BITS = {INT_MAX_DIGITS, INT_MAX_DIGITS, INT_MAX_DIGITS};
const int GlobalAttributeGroupId::ID_LENGTH = std::accumulate(COMPOSITE_BITS.begin(), COMPOSITE_BITS.end(), 0);