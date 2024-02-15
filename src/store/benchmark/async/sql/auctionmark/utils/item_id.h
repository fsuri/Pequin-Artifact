#ifndef AUCTIONMARK_ITEM_ID_H
#define AUCTIONMARK_ITEM_ID_H

#include <string>
#include <array>
#include <sstream>
#include <algorithm>
#include <functional>
#include <vector>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/user_id.h"

namespace auctionmark
{

  class ItemId : CompositeId
  {
  private:
    static const std::vector<int> COMPOSITE_BITS;

    UserId seller_id;
    int item_ctr;

    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
      ar & seller_id;
      ar & item_ctr;
    }

  public:
    ItemId(const std::string &seller_id, int item_ctr);
    ItemId(const UserId &seller_id, int item_ctr);
    ItemId(const std::string &composite_id);
    ItemId(const ItemId &other);
    ItemId() = default;
    ~ItemId() = default;

    std::string encode() const;
    void decode(const std::string &composite_id);
    std::vector<std::string> to_vec() const;

    UserId get_seller_id() const;
    int get_item_ctr() const;

    std::string to_string() const;
    bool operator==(const ItemId &other) const;
    bool operator<(const ItemId &other) const;
  };

} // namespace auctionmark

#endif // AUCTIONMARK_ITEM_ID_H