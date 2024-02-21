#ifndef AUCTIONMARK_GLOBAL_ATTRIBUTE_VALUE_ID_H
#define AUCTIONMARK_GLOBAL_ATTRIBUTE_VALUE_ID_H

#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_group_id.h"

namespace auctionmark {

class GlobalAttributeValueId : CompositeId {
private:
    static const std::vector<int> COMPOSITE_BITS;

    std::string group_attribute_id;
    int id;

public:
    GlobalAttributeValueId(const std::string& group_attribute_id, int id);
    GlobalAttributeValueId(const GlobalAttributeGroupId& group_attribute_id, int id);

    std::string encode() const;
    void decode(const std::string& composite_id);
    std::vector<std::string> to_vec() const;

    GlobalAttributeGroupId get_global_attribute_group() const;
    int get_id() const;

    bool operator==(const GlobalAttributeValueId& other) const;
    bool operator<(const GlobalAttributeValueId& other) const;
};

} // namespace auctionmark

#endif // AUCTIONMARK_GLOBAL_ATTRIBUTE_VALUE_ID_H