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
    LoaderItemInfo(ItemId itemId, uint64_t endDate, uint64_t numBids) : ItemInfo(itemId, 0.0, endDate, numBids), purchaseDate(0) {}
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


LoaderItemInfo GenerateItemTableRow(TableWriter &writer, AuctionMarkProfile &profile, std::mt19937_64 &gen, const UserId &seller_id, int remaining);

void GenerateSubTableRows(TableWriter &writer, AuctionMarkProfile &profile, std::mt19937_64 &gen, LoaderItemInfo &itemInfo);
void GenerateItemImageRow(TableWriter &writer, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);
void GenerateItemAttributeRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);
void GenerateItemCommentRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);
void GenerateItemBidRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo);
void GenerateItemMaxBidRow(TableWriter &writer, LoaderItemInfo &itemInfo);
void GenerateItemPurchaseRow(TableWriter &writer, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);
void GenerateUserFeedbackRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);
void GenerateUserItemRow(TableWriter &writer, LoaderItemInfo &itemInfo);
void GenerateUserWatchRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo, std::mt19937_64 &gen);

uint64_t getRandomStartTimestamp(uint64_t endDate, AuctionMarkProfile &profile) {
  uint64_t duration =  ((uint64_t) profile.get_random_duration()) * MILLISECONDS_IN_A_DAY;
  uint64_t lStartTimestamp = endDate- duration;
  return lStartTimestamp;
}


uint64_t getRandomEndTimestamp(AuctionMarkProfile &profile) {
  int64_t timeDiff =  profile.get_random_time_diff() * MILLISECONDS_IN_A_SECOND;
  uint64_t EndTimestamp = profile.get_loader_start_time() + timeDiff; 

  // std::cerr << "time diff: " <<  timeDiff << std::endl;
  // std::cerr << "end date:  " <<  EndTimestamp << std::endl;
  // std::cerr << "loadstart: " << profile.get_loader_start_time() << std::endl;
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



//OLD SEQUENTIAL GENERATORS

/*
void GenerateItemImage(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_IMAGE;
  
  //DATA GEN
  for(auto &item: items){
    for(int count = 0; count < item.numImages; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ii_id   //FIXME: Unclear if this is the correct use of count
      values.push_back(item.get_item_id().encode()); //ii_i_id
      values.push_back(item.get_seller_id().encode()); //ii_u_id
      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemAttribute(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_ATTR;
  
  //DATA GENERATION
  for(auto &item: items){
    for(int count = 0; count < item.numAttributes; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ia_id  //FIXME: Unclear if this is the correct use of count
      values.push_back(item.get_item_id().encode()); //ia_i_id
      values.push_back(item.get_seller_id().encode()); //ia_u_id
    
      GlobalAttributeValueId gav_id = profile.get_random_global_attribute_value();
      values.push_back(gav_id.encode()); //ia_gav_id
      values.push_back(gav_id.get_global_attribute_group().encode()); //ia_gag_id
      writer.add_row(table_name, values);
    }
  }
}



void GenerateItemComment(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_COMMENT;
  
  //DATA GENERATION
  std::mt19937_64 gen;

  for(auto &itemInfo: items){
    int total = itemInfo.purchaseDate > 0 ? itemInfo.numComments : 0;
    for(int count = 0; count < total; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ic_id     //FIXME: Unclear if this is the correct use of count
      values.push_back(itemInfo.get_item_id().encode()); //ic_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //ic_u_id
      values.push_back(itemInfo.lastBidderId.encode()); //ic_buyer_id

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_question

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_response
      
      uint64_t t = getRandomCommentDate(itemInfo.startDate, itemInfo.get_end_date(), gen);
      values.push_back(std::to_string(t));//ic_created
      values.push_back(std::to_string(t));//ic_updated

      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemBid(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
   std::string table_name = TABLE_ITEM_BID;
 
   //DATA GENERATION
  for(auto &itemInfo: items){
    Bid bid;
    bool new_item = true;
    //TODO: Should these three not reset between items?
    float currentPrice;
    float currentBidPriceAdvanceStep;
    uint64_t currentCreateDateAdvanceStep;

    int total = itemInfo.get_num_bids();
    for(int count = 0; count < total; ++count){
      int remaining = total - count - 1;

      UserId bidderId;
      // Figure out the UserId for the person bidding on this item now

      if(new_item) {
        // If this is a new item and there is more than one bid, then  we'll choose the bidder's UserId at random.
        // If there is only one bid, then it will have to be the last bidder
        bidderId = itemInfo.get_num_bids() == 1 ? itemInfo.lastBidderId : profile.get_random_buyer_id(itemInfo.get_seller_id());
        uint64_t endDate;
        if(itemInfo.get_status() == ItemStatus::OPEN){
          endDate = profile.get_loader_start_time();
        }
        else{
          endDate = itemInfo.get_end_date();
        }
        currentCreateDateAdvanceStep = (endDate - itemInfo.startDate) / (remaining + 1);
        currentBidPriceAdvanceStep = itemInfo.initialPrice * ITEM_BID_PERCENT_STEP;
        currentPrice = itemInfo.initialPrice;
      }
      else if(count == total){
         // The last bid must always be the item's lastBidderId
         bidderId = itemInfo.lastBidderId;
         currentPrice = itemInfo.get_current_price();
      }
      else if(total == 2){
         // The first bid for a two-bid item must always be different than the lastBidderId
        bidderId = profile.get_random_buyer_id({itemInfo.lastBidderId, itemInfo.get_seller_id()});
      }
      else{
         // Since there are multiple bids, we want randomly select one based on the previous bidders
        // We will get the histogram of bidders so that we are more likely to select an existing bidder rather than a completely random one
        auto &bidderHistogram = itemInfo.bidderHistogram;
        bidderId = profile.get_random_buyer_id(bidderHistogram, {bid.bidderId, itemInfo.get_seller_id()});
        currentPrice += currentBidPriceAdvanceStep;
      }

      //Update bid info
      float last_bid = new_item? itemInfo.initialPrice : bid.maxBid;
      bid = itemInfo.getNextBid(count, bidderId);
      bid.createDate = itemInfo.startDate + currentCreateDateAdvanceStep;
      bid.updateDate = bid.createDate;

      if(remaining == 0){
        bid.maxBid = itemInfo.get_current_price();
      }
      else{
        bid.maxBid = last_bid + currentBidPriceAdvanceStep;
      }


      //ROW generation
      std::vector<std::string> values;

      values.push_back(std::to_string(bid.id)); //ib_id
      values.push_back(itemInfo.get_item_id().encode()); //ib_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //ib_u_id
      values.push_back(bid.bidderId.encode()); //ib_buyer_id
      float price = bid.maxBid - (remaining > 0 ? currentBidPriceAdvanceStep / 2.0 : 0);
      values.push_back(std::to_string(price)); //ib_bid
      values.push_back(std::to_string(bid.maxBid)); //ib_max_bid
      values.push_back(std::to_string(bid.createDate)); //ib_created
      values.push_back(std::to_string(bid.updateDate)); //ib_updated
    
      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemMaxBid(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_MAX_BID;
  
   //DATA GENERATION
  for(auto &itemInfo: items){
    bool has_max_bid = itemInfo.bids.size() > 0 ? 1 : 0;
    if(has_max_bid){
      Bid const &bid = itemInfo.getLastBid();

      std::vector<std::string> values;
     
       // IMB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
      
      // IMB_U_ID
      values.push_back(itemInfo.get_seller_id().encode());
    
      // IMB_IB_ID
      values.push_back(std::to_string(bid.id));
      
      // IMB_IB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
     
      // IMB_IB_U_ID
       values.push_back(itemInfo.get_seller_id().encode());

      // IMB_CREATED
      values.push_back(std::to_string(bid.createDate));
     
      // IMB_UPDATED
      values.push_back(std::to_string(bid.updateDate));
     
      writer.add_row(table_name, values);
    }
  }
}

 void GenerateItemPurchase(TableWriter &writer, std::vector<LoaderItemInfo> &items){
   std::string table_name = TABLE_ITEM_PURCHASE;

   //DATA GENERATION
  std::mt19937_64 gen;

  for(auto &itemInfo: items){  
    bool has_purchase = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(has_purchase){
      Bid &bid = itemInfo.getLastBid();

      std::vector<std::string> values;
     
       // IP_ID
      values.push_back(std::to_string(0)); // //FIXME: Unclear if this is the correct use of count
      
    
      // IP_IB_ID
      values.push_back(std::to_string(bid.id));
      
      // IP_IB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
     
      // IP_IB_U_ID
       values.push_back(itemInfo.get_seller_id().encode());

      // IP_DATE
      values.push_back(std::to_string(itemInfo.purchaseDate));
     
      // IMB_UPDATED
      values.push_back(std::to_string(bid.updateDate));
     
      writer.add_row(table_name, values);

      if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_BUYER_LEAVES_FEEDBACK){
        bid.buyer_feedback = true;
      }
      if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_SELLER_LEAVES_FEEDBACK){
        bid.seller_feedback = true;
      }

    }
  }
}

//////////////////


void GenerateUserFeedback(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_USERACCT_FEEDBACK;
  
  //DATA GEN
  for(auto &itemInfo: items){  
    bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(!was_sold) continue;

    Bid const &bid = itemInfo.getLastBid();

    if(bid.buyer_feedback){
      std::vector<std::string> values;
     
      values.push_back(bid.bidderId.encode()); // uf_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_from_id
      values.push_back(std::to_string(1)); //uf_rating
      values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
      
      writer.add_row(table_name, values);
    }

    if(bid.seller_feedback){
      std::vector<std::string> values;
     
      values.push_back(itemInfo.get_seller_id().encode()); // uf_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
      values.push_back(bid.bidderId.encode()); //uf_from_id
      values.push_back(std::to_string(1)); //uf_rating
      values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
      
      writer.add_row(table_name, values);
    }
  }
}

void GenerateUserItem(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_USERACCT_ITEM;
  
  //DATA GEN
  for(auto &itemInfo: items){  
    bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(!was_sold) continue;

    Bid const &bid = itemInfo.getLastBid();

    std::vector<std::string> values;
    
    values.push_back(bid.bidderId.encode()); // ui_u_id
    values.push_back(itemInfo.get_item_id().encode()); //ui_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ui_i_u_id
    //TODO: Technically these are all "null"
    values.push_back(std::to_string(-1)); //ui_ip_id
    values.push_back(std::to_string(-1)); //ui_ip_ib_id
    values.push_back("\"\""); //ui_ip_ib_i_id
    values.push_back("\"\"");//ui_ip_ib_u_id
    values.push_back(std::to_string(itemInfo.get_end_date()));//ui_created
    
    writer.add_row(table_name, values);
  }
}

void GenerateUserWatch(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
   
  std::string table_name = TABLE_USERACCT_WATCH;

  //DATA GEN
  std::mt19937_64 gen;

  for(auto &itemInfo: items){  
    
    std::set<UserId> watchers; 

    for(int i = 0; i < itemInfo.numWatches; ++i){
      auto &bidderHistogram = itemInfo.bidderHistogram;
      UserId buyerId;
      bool use_random = itemInfo.numWatches == bidderHistogram.size();
      uint64_t num_watchers = watchers.size();
      uint64_t num_users = TABLESIZE_USERACCT;

      int tries = 1000; //find new watcher
      while(num_watchers < num_users && tries-- > 0){
        if(use_random){
          buyerId = profile.get_random_buyer_id();
        }
        else{
          buyerId = profile.get_random_buyer_id(bidderHistogram, {itemInfo.get_seller_id()});
        }
        if(watchers.insert(buyerId).second) break;
        buyerId = UserId();

         // If for some reason we unable to find a buyer from our bidderHistogram, then just give up and get a random one
        if(!use_random && tries == 0){
          use_random = true;
          tries = 500;
        }
      }

      //Generate row
      std::vector<std::string> values;
    
      values.push_back(buyerId.encode()); // uw_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uw_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uw_i_u_id
      values.push_back(std::to_string(getRandomDate(itemInfo.startDate, itemInfo.get_end_date(), gen)));//uw_created
      
      writer.add_row(table_name, values);
    }
  }
}
 
} //namespace auctionmark



int main(int argc, char *argv[]) {

  auto start_time = std::time(0);
  
  gflags::SetUsageMessage("generates a json file containing sql tables for AuctionMark data\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string file_name = "sql-auctionmark";
  TableWriter writer = TableWriter(file_name);

  std::cerr << "Starting AUCTIONMARK Table Generation. Num Clients: " << FLAGS_client_total << ". Scale Factor: " << FLAGS_scale_factor << std::endl;

  std::mt19937_64 gen;
 
  auctionmark::AuctionMarkProfile profile(-1, FLAGS_client_total, FLAGS_scale_factor);
  struct timeval time;
  gettimeofday(&time, NULL);
  profile.set_loader_start_time(auctionmark::get_ts(time));

  std::cerr << "loader_start_time: " << profile.get_loader_start_time() << std::endl;

  
  auctionmark::GenerateRegionTable(writer);
  int n_categories = auctionmark::GenerateCategoryTable(writer);
  int n_gags = auctionmark::GenerateGlobalAttributeGroupTable(writer, n_categories, profile);
  auctionmark::GenerateGlobalAttributeValueTable(writer, profile, n_gags);

  std::cerr << "Finished General Tables" << std::endl;

  //Generate UserTables
  std::vector<auctionmark::UserId> users = auctionmark::GenerateUserAcctTable(writer, profile);
  std::cerr << "Finished UserAcct Table" << std::endl;

  std::vector<auctionmark::LoaderItemInfo> items = auctionmark::GenerateItemTable(writer, profile, users);
  std::cerr << "Finished Item Table" << std::endl;

  // auctionmark::GenerateItemImage(writer, items);
  // auctionmark::GenerateItemAttribute(writer, profile, items);
  // auctionmark::GenerateItemComment(writer, items);
  // auctionmark::GenerateItemBid(writer, profile, items);
  // auctionmark::GenerateItemMaxBid(writer, items);
  // auctionmark::GenerateItemPurchase(writer, items);

  // std::cerr << "Finished Item* Tables" << std::endl;

  // auctionmark::GenerateUserFeedback(writer, profile, items);
  // auctionmark::GenerateUserItem(writer, items);
  // auctionmark::GenerateUserWatch(writer, profile, items);
  // std::cerr << "Finished User* Tables" << std::endl;

  // //TODO: Serialize profile.
  //  profile.set_loader_stop_time(std::chrono::system_clock::now());

  writer.flush();
  // std::cerr << "Wrote tables." << std::endl;

   auto end_time = std::time(0);
    std::cerr << "Finished AUCTIONMARK Table Generation. Took " << (end_time - start_time) << "seconds" << std::endl;
  return 0;
}
*/
