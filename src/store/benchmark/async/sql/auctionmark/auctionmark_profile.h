#ifndef AUCTIONMARK_PROFILE_H
#define AUCTIONMARK_PROFILE_H

#include <vector>
#include <string>
#include <random>
#include <utility>
#include <optional>
#include <unordered_set>
#include <boost/histogram.hpp>
#include <sys/time.h>
#include "store/common/frontend/sync_client.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_info.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_value_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/user_id_generator.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_comment_response.h"
#include "store/benchmark/async/sql/auctionmark/utils/zipf.h"
#include "store/benchmark/async/sql/auctionmark/utils/flat_histogram.h"

namespace auctionmark
{

  struct ItemRecord
  {
    ItemRecord() {}
    ItemRecord(std::string itemId, std::string sellerId, std::string i_name, double currentPrice,
              double numBids, uint64_t endDate, ItemStatus itemStatus)
              : itemId(itemId), sellerId(sellerId), i_name(i_name), currentPrice(currentPrice),
                 numBids(numBids), endDate(endDate), itemStatus(itemStatus)
    {}
    ItemRecord(std::string itemId, std::string sellerId, std::string i_name, double currentPrice,
              double numBids, uint64_t endDate, ItemStatus itemStatus, uint64_t bidId, std::string buyerId)
              : itemId(itemId), sellerId(sellerId), i_name(i_name), currentPrice(currentPrice),
                 numBids(numBids), endDate(endDate), itemStatus(itemStatus), bidId(bidId), buyerId(buyerId)
    {}
    std::string itemId;
    std::string sellerId;
    std::string i_name;
    double currentPrice;
    double numBids;
    uint64_t endDate;
    ItemStatus itemStatus;
    uint64_t bidId;
    std::string buyerId;
  };


  class AuctionMarkProfile
  {
    // using int_hist_t = boost::histogram::histogram<std::tuple<boost::histogram::axis::integer<>>>;
    // using str_cat_hist_t = boost::histogram::histogram<std::tuple<boost::histogram::axis::category<std::string>>>;

  public:
    //AuctionMarkProfile();
    AuctionMarkProfile(int client_id, int num_clients, double scale_factor);
    inline ~AuctionMarkProfile()
    {
      clear_cached_profile();
    }

    /* Time methods */
    uint64_t get_scaled_current_timestamp(uint64_t& time);
    uint64_t update_and_get_current_time();
    uint64_t get_current_time();
    uint64_t get_loader_start_time();
    void set_loader_start_time(uint64_t start_time);
    uint64_t get_loader_stop_time();
    void set_loader_stop_time(uint64_t stop_time);
    uint64_t set_and_get_client_start_time();
    uint64_t get_client_start_time();
    bool has_client_start_time();
    uint64_t update_and_get_last_close_auctions_time();
    uint64_t get_last_close_auctions_time();
    int get_random_time_diff();
    int get_random_duration();

    /* General Methods */
    double get_scale_factor();
    void set_scale_factor(double scale_factor);

    /* User Methods */
    UserId get_random_user_id(int min_item_count, int client_id, std::vector<UserId> &exclude);

    UserId get_random_buyer_id();
    UserId get_random_buyer_id(UserId exclude);
    UserId get_random_buyer_id(std::vector<UserId> exclude);
    UserId get_random_buyer_id(std::map<UserId, uint64_t>  &previous_bidders, std::vector<UserId> exclude);

    //UserId get_random_buyer_id(str_cat_hist_t &previous_bidders, std::vector<UserId> &exclude);
    //UserId get_random_buyer_id(histogram_str &previous_bidders, std::vector<UserId> &exclude);
    UserId get_random_seller_id(int client);
    void add_pending_item_comment_response(ItemCommentResponse &cr);

    /* Item Methods */
    ItemId get_next_item_id(UserId &seller_id);
    bool add_item(std::vector<ItemInfo> &items, ItemInfo &item_info);
    void update_item_queues();
    std::optional<ItemStatus> add_item_to_proper_queue(ItemInfo &item_info, bool is_loader);
    std::optional<ItemStatus> add_item_to_proper_queue(ItemInfo &item_info, uint64_t &base_time, std::optional<std::pair<std::vector<ItemInfo>::iterator, std::vector<ItemInfo>>> current_queue_iterator, bool is_loader = false);
    std::optional<ItemInfo> get_random_item(std::vector<ItemInfo> item_set, bool need_current_price, bool need_future_end_date);

    /* Available Items */
    std::optional<ItemInfo> get_random_available_item();
    std::optional<ItemInfo> get_random_available_item(bool has_current_price);
    int get_available_items_count();

    /* Ending Soon Items */
    std::optional<ItemInfo> get_random_ending_soon_item();
    std::optional<ItemInfo> get_random_ending_soon_item(bool has_current_price);
    int get_ending_soon_items_count();

    /* Waiting For Purchase Items */
    std::optional<ItemInfo> get_random_waiting_for_purchase_item();
    int get_waiting_for_purchase_items_count();

    /* Completed Items */
    std::optional<ItemInfo> get_random_completed_item();
    int get_completed_items_count();

    /* All Items */
    int get_all_items_count();
    std::optional<ItemInfo> get_random_item();

    /* Global Attribute Methods */
    GlobalAttributeValueId get_random_global_attribute_value();
    int get_random_category_id();

    ItemId processItemRecord(ItemRecord &row);

    /* Serialization Methods */
    void save_profile(SyncClient &client);
    void copy_profile(int client_id, const AuctionMarkProfile &other);

    static void clear_cached_profile();

    void load_profile(int client_id);


    inline int get_client_id(){
      return client_id;
    }

    inline int num_pending_comment_responses(){
      return pending_comment_responses.size();
    }

    std::vector<ItemCommentResponse> pending_comment_responses;

    std::map<std::string, int> ip_id_cntrs;

    std::optional<UserIdGenerator> user_id_generator;

    std::vector<GlobalAttributeGroupId> gag_ids;

    //std::binomial_distribution<int> random_time_diff;
    GaussGenerator random_time_diff;
    //std::binomial_distribution<int> random_duration;
    GaussGenerator random_duration;
    Zipf random_num_images;
    Zipf random_num_attributes;
    Zipf random_purchase_duration;
    Zipf random_num_comments;
    Zipf random_initial_price;

    //std::vector<int> users_per_item_count;  //A histogram for the number of users that have the number of items listed ItemCount -> # of Users
    std::map<int, int> users_per_item_count;

  private:
    static AuctionMarkProfile *cached_profile;
    int client_id;
    int num_clients;
    double scale_factor;
    std::mt19937_64 gen;
    uint64_t loader_start_time;
    uint64_t loader_stop_time;

   
    histogram_int items_per_category;

    std::vector<ItemInfo> items_available;
    std::vector<ItemInfo> items_ending_soon;
    std::vector<ItemInfo> items_waiting_for_purchase;
    std::vector<ItemInfo> items_completed;

    std::vector<ItemInfo> all_item_sets[ITEM_SETS_NUM] = {
        items_available,
        items_ending_soon,
        items_waiting_for_purchase,
        items_completed};

    

  

    std::optional<FlatHistogram_Int> random_category;
    std::optional<FlatHistogram_Int> random_item_count;
    // FlatHistogram_Int random_category;
    // FlatHistogram_Int random_item_count;

    uint64_t last_close_auctions_time;
    uint64_t client_start_time;
    uint64_t current_time;

    //str_cat_hist_t seller_item_cnt;
    std::map<std::string, int> seller_item_cnt;

    inline void initialize_user_id_generator(int client_id)
    {
      user_id_generator = UserIdGenerator(users_per_item_count, num_clients, client_id);
    }
  };

} // namespace auctionmark

#endif //AUCTIONMARK_PROFILE_H
