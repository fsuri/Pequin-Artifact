#ifndef AUCTIONMARK_GLOBAL_ATTRIBUTE_GROUP_ID_H
#define AUCTIONMARK_GLOBAL_ATTRIBUTE_GROUP_ID_H

#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"

namespace auctionmark {

class GlobalAttributeGroupId : CompositeId {
private:
    static const std::vector<int> COMPOSITE_BITS;
    int category_id;
    int id;
    int count;

public:
    GlobalAttributeGroupId(int category_id, int id, int count);
    GlobalAttributeGroupId(const std::string& composite_id);

    std::string encode() const;
    void decode(const std::string& composite_id);
    std::vector<std::string> to_vec() const;

    int get_category_id() const;
    int get_id() const;
    int get_count() const;

    bool operator==(const GlobalAttributeGroupId& other) const;

    static const int ID_LENGTH;
};

} // namespace auctionmark

#endif // AUCTIONMARK_GLOBAL_ATTRIBUTE_GROUP_ID_H