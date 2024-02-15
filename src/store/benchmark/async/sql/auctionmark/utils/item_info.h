#ifndef AUCTIONMARK_ITEM_INFO_H
#define AUCTIONMARK_ITEM_INFO_H

#include <string>
#include <optional>
#include <chrono>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/utility.hpp>
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_status.h"

namespace auctionmark
{

  class ItemInfo
  {
  private:
    ItemId item_id;
    std::optional<double> current_price;
    std::optional<std::chrono::system_clock::time_point> end_date;
    long num_bids;
    ItemStatus status;

    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
      ar & item_id;
      ar & current_price;
      ar & end_date;
      ar & num_bids;
      ar & status;
    }

  public:
    ItemInfo(ItemId id, std::optional<double> current_price, std::optional<std::chrono::system_clock::time_point> end_date, int num_bids);
    ItemInfo(std::string id, double current_price, uint64_t end_date, int num_bids);
    ItemInfo();
    ~ItemInfo() = default;

    ItemId get_item_id() const;
    UserId get_seller_id() const;
    bool has_current_price() const;
    std::optional<float> get_current_price() const;
    bool has_end_date() const;
    std::optional<std::chrono::system_clock::time_point> get_end_date() const;
    void set_item_id(ItemId item_id);
    void set_current_price(std::optional<float> current_price);
    void set_end_date(std::optional<std::chrono::system_clock::time_point> end_date);
    long get_num_bids() const;
    void set_num_bids(long num_bids);
    ItemStatus get_status() const;
    void set_status(ItemStatus status);
    bool operator==(const ItemInfo& other) const;
    bool operator<(const ItemInfo& other) const;
};

} // namespace auctionmark

#endif // AUCTIONMARK_ITEM_INFO_H