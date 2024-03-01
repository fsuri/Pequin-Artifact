#ifndef AUCTIONMARK_GLOBAL_ATTRIBUTE_GROUP_ID_H
#define AUCTIONMARK_GLOBAL_ATTRIBUTE_GROUP_ID_H

#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"

namespace auctionmark {

class GlobalAttributeGroupId : CompositeId {
private:
    static const std::vector<int> COMPOSITE_BITS;
    int category_id;
    int id;
    int count;

    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & category_id;
        ar & id;
        ar & count;
    }

public:
    GlobalAttributeGroupId(int category_id, int id, int count);
    GlobalAttributeGroupId(const std::string& composite_id);
    GlobalAttributeGroupId() = default;
    ~GlobalAttributeGroupId() = default;

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