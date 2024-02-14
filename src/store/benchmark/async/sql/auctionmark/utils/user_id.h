#ifndef AUCTIONMARK_USER_ID_H
#define AUCTIONMARK_USER_ID_H

#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"

namespace auctionmark
{

  class UserId : CompositeId
  {
  private:
    static const std::vector<int> COMPOSITE_BITS;
    int item_count;
    int offset;

  public:
    static const int ID_LENGTH;

    UserId(int item_count, int offset);
    UserId(std::string composite_id);
    UserId(const UserId &other);
    UserId();

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

    struct HashFunction
    {
      size_t operator()(const UserId &point) const;
    };
  };

} // namespace auctionmark

#endif // AUCTIONMARK_USER_ID_H