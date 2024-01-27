#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include "composite_id.h"

class UserId : CompositeId
{
private:
    static const int COMPOSITE_BITS[2];

    int itemCount;
    int offset;

public:
    UserId(int itemCount, int offset) : itemCount(itemCount), offset(offset) {
    }

    UserId(std::string composite_id)
    {
        decode(composite_id);
    }

    UserId(UserId& other) : itemCount(other.itemCount), offset(other.offset) {}
    UserId() : itemCount(0), offset(0) {}

    void initialize_composite_bits() {
        COMPOSITE_BITS[0] = INT_MAX_DIGITS;
        COMPOSITE_BITS[1] = INT_MAX_DIGITS;
    }

    std::string encode() const
    {
        const std::vector<int> composite_bits = {COMPOSITE_BITS[0], COMPOSITE_BITS[1]};
        return CompositeId::encode(composite_bits);
    }

    void decode(const std::string& composite_id)
    {
        std::vector<int> composite_bits = {COMPOSITE_BITS[0], COMPOSITE_BITS[1]};
        std::vector<std::string> decodedValues = CompositeId::decode(composite_id, composite_bits);
        itemCount = std::stoi(decodedValues[0]);
        offset = std::stoi(decodedValues[1]);
    }

    std::vector<std::string> to_array() const
    {
        return {std::to_string(itemCount), std::to_string(offset)};
    }

    int getItemCount() const
    {
        return itemCount;
    }

    int getOffset() const
    {
        return offset;
    }

    std::string toString() const
    {
        std::stringstream ss;
        ss << "UserId<itemCount=" << itemCount << ", offset=" << offset << ", encoded=" << encode() << ">";
        return ss.str();
    }

    static std::string toString(std::string userId)
    {
        return UserId(userId).toString();
    }

    bool operator==(const UserId &other) const
    {
        return itemCount == other.itemCount && offset == other.offset;
    }

    bool operator!=(const UserId &other) const
    {
        return !(*this == other);
    }

    bool operator<(const UserId &other) const
    {
        if (itemCount != other.itemCount)
        {
            return itemCount < other.itemCount;
        }
        return offset < other.offset;
    }
};