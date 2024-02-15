/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <random>
#include <set>
// #include <boost/histogram.hpp>

#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

#include "store/benchmark/async/sql/auctionmark/utils/category_parser.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/flat_histogram.h"

#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_group_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_value_id.h"


namespace auctionmark{

  static std::map<uint64_t, std::pair<Zipf, Zipf>> item_bid_watch_zipfs;


class Bid {
  public: 
    Bid() {}
    Bid(uint64_t id, UserId bidderId): id(id), bidderId(bidderId), maxBid(0) {}
    ~Bid(){}
    uint64_t id;
    UserId bidderId;
    float maxBid;
    uint64_t createDate;
    uint64_t updateDate;
    bool buyer_feedback = false;
    bool seller_feedback = false;
};

class LoaderItemInfo : public ItemInfo {
  public:
    LoaderItemInfo(ItemId itemId, uint64_t endDate, uint64_t numBids) : ItemInfo(itemId, std::nullopt, endDate, numBids) {}
    ~LoaderItemInfo(){}
    std::vector<Bid> bids;
    std::map<UserId, uint64_t> bidderHistogram;
    //FlatHistogram_Str bidderHistogram; //hold encoded UserId; //TODO: Rename FlatHistogram to Histogram; Template it.
    //Histogram<UserId> bidderHistogram;

    uint32_t numImages;
    uint32_t numAttributes;
    uint32_t numComments;
    uint64_t numWatches;
    uint64_t startDate;
    uint64_t purchaseDate;
    float initialPrice;
    UserId lastBidderId; //if null, then no bidder

    Bid getNextBid(uint64_t id, UserId bidder_id){
      Bid b(id, bidder_id);
      bids.push_back(std::move(b));
      bidderHistogram[bidder_id]++;
      return b;
    }

    Bid& getLastBid(){
      return bids[bids.size()-1];
    }
};

uint64_t getRandomStartTimestamp(uint64_t endDate, AuctionMarkProfile &profile) {
  uint64_t duration =  ((uint64_t) profile.get_random_duration()) * MILLISECONDS_IN_A_DAY;
  uint64_t lStartTimestamp = endDate- duration;
  return lStartTimestamp;
}

uint64_t getRandomEndTimestamp(AuctionMarkProfile &profile) {
  int timeDiff =  profile.get_random_time_diff();
  uint64_t EndTimestamp = profile.get_loader_start_time() + timeDiff; 
  assert(EndTimestamp > 0);
  return EndTimestamp;
}

uint64_t getRandomPurchaseTimestamp(uint64_t endDate, AuctionMarkProfile &profile) {
  uint64_t duration =  profile.random_purchase_duration.next_long();
  uint64_t lStartTimestamp = endDate + duration * MILLISECONDS_IN_A_DAY;
  return lStartTimestamp;
}

uint64_t getRandomCommentDate(uint64_t startDate, uint64_t endDate, std::mt19937_64 &gen) {
  uint64_t start = round(startDate / MILLISECONDS_IN_A_SECOND);
  uint64_t end = round(endDate / MILLISECONDS_IN_A_SECOND);
  return std::uniform_int_distribution<uint64_t>(start, end)(gen) * MILLISECONDS_IN_A_SECOND;
}

uint64_t getRandomDate(uint64_t startDate, uint64_t endDate, std::mt19937_64 &gen) {
  uint64_t start = round(startDate / MILLISECONDS_IN_A_SECOND);
  uint64_t end = round(endDate / MILLISECONDS_IN_A_SECOND);
  uint64_t offset = std::uniform_int_distribution<uint64_t>(start, end)(gen);
  return offset * MILLISECONDS_IN_A_SECOND;
}

} //namespace auctionmark

