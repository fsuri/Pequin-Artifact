#ifndef AUCTIONMARK_USER_ID_H
#define AUCTIONMARK_USER_ID_H

#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <boost/container_hash/hash.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"

namespace auctionmark
{

  class UserId : CompositeId
  {
  private:
    static const std::vector<int> COMPOSITE_BITS;
    int item_count;
    int offset;

    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
      ar & item_count;
      ar & offset;
    }

  public:
    static const int ID_LENGTH;

    UserId(int item_count, int offset);
    UserId(std::string composite_id);
    UserId(const UserId &other);
    UserId();
    ~UserId() = default;

    std::string encode() const;
    void decode(const std::string &composite_id);
    std::vector<std::string> to_vec() const;
    int get_item_count() const;
    int get_offset() const;
    std::string to_string() const;
    static std::string to_string(std::string user_id);
    bool operator==(const UserId &other) const;
    bool operator!=(const UserId &other) const;
    bool operator<(const UserId &other) const;
    std::size_t hash_value(UserId const& user_id);
  };

} // namespace auctionmark

// template <>
// struct std::hash<auctionmark::UserId>
// {
//   std::size_t operator()(const auctionmark::UserId& user_id) const
//   {
//     std::size_t seed = 0;
//     boost::hash_combine(seed, user_id.get_item_count());
//     boost::hash_combine(seed, user_id.get_offset());
//     return seed;
//   }
// };

#endif // AUCTIONMARK_USER_ID_H