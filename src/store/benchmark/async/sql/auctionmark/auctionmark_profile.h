#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <utility>
#include <optional>
#include <unordered_set>
#include <boost/histogram.hpp>
#include "store/common/frontend/sync_client.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_info.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_group_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_value_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/user_id_generator.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_comment_response.h"
#include "store/benchmark/async/sql/auctionmark/utils/zipf.h"
#include "store/benchmark/async/sql/auctionmark/utils/flat_histogram.h"

namespace auctionmark
{

  class AuctionMarkProfile
  {
    using int_hist_t = boost::histogram::histogram<
        std::tuple<
            boost::histogram::axis::integer<>>>;
    using str_cat_hist_t = boost::histogram::histogram<
        std::tuple<
            boost::histogram::axis::category<std::string>>>;

  public:
    AuctionMarkProfile(int client_id, double scale_factor, int num_clients, std::mt19937_64 gen);
    inline ~AuctionMarkProfile()
    {
      clear_cached_profile();
    }

    /* Time methods */
    std::chrono::system_clock::time_point get_scaled_current_timestamp(std::chrono::system_clock::time_point time);
    std::chrono::system_clock::time_point update_and_get_current_time();
    std::chrono::system_clock::time_point get_current_time();
    std::chrono::system_clock::time_point get_loader_start_time();
    std::chrono::system_clock::time_point get_loader_stop_time();
    std::chrono::system_clock::time_point set_and_get_client_start_time();
    std::chrono::system_clock::time_point get_client_start_time();
    bool has_client_start_time();
    std::chrono::system_clock::time_point update_and_get_last_close_auctions_time();
    std::chrono::system_clock::time_point get_last_close_auctions_time();

    /* General Methods */
    double get_scale_factor();
    void set_scale_factor(double scale_factor);

    /* User Methods */
    UserId get_random_user_id(int min_item_count, int client_id, std::vector<UserId> &exclude);
    UserId get_random_buyer_id(std::vector<UserId> &exclude);

    UserId get_random_buyer_id(str_cat_hist_t &previous_bidders, std::vector<UserId> &exclude);
    UserId get_random_seller_id(int client);
    void add_pending_item_comment_response(ItemCommentResponse &cr);

    /* Item Methods */
    ItemId get_next_item_id(UserId &seller_id);
    bool add_item(std::vector<ItemInfo> &items, ItemInfo &item_info);
    void update_item_queues();
    std::optional<ItemStatus> add_item_to_proper_queue(ItemInfo &item_info, bool is_loader);
    std::optional<ItemStatus> add_item_to_proper_queue(ItemInfo &item_info, std::chrono::system_clock::time_point &base_time, std::optional<std::pair<std::vector<ItemInfo>::iterator&, std::vector<ItemInfo>&>> current_queue_iterator);
    std::optional<ItemInfo> get_random_item(std::vector<ItemInfo> item_set, bool need_current_price, bool need_future_end_date);
    long get_random_num_images();
    long get_random_num_attributes();
    long get_random_purchase_duration();
    long get_random_num_comments();
    long get_random_initial_price();

    /* Available Items */
    ItemInfo get_random_available_item();
    ItemInfo get_random_available_item(bool has_current_price);
    int get_available_items_count();

    /* Ending Soon Items */
    ItemInfo get_random_ending_soon_item();
    ItemInfo get_random_ending_soon_item(bool has_current_price);
    int get_ending_soon_items_count();

    /* Waiting For Purchase Items */
    ItemInfo get_random_waiting_for_purchase_item();
    int get_waiting_for_purchase_items_count();

    /* Completed Items */
    ItemInfo get_random_completed_item();
    int get_completed_items_count();

    /* All Items */
    int get_all_items_count();
    ItemInfo get_random_item();

    /* Global Attribute Methods */
    GlobalAttributeValueId get_random_global_attribute_value();
    int get_random_category_id();

    /* Serialization Methods */
    void save_profile(SyncClient &client);
    void copy_profile(int client_id, AuctionMarkProfile &other);

    static void clear_cached_profile()
    {
      if (cached_profile != nullptr)
      {
        delete cached_profile;
        cached_profile = nullptr;
      }
    }

    void load_profile(int client_id);

  private:
    static AuctionMarkProfile *cached_profile;
    const int client_id;
    std::mt19937_64 gen;
    const int num_clients;
    double scale_factor;
    std::chrono::system_clock::time_point loader_start_time;
    std::chrono::system_clock::time_point loader_stop_time;

    int_hist_t users_per_item_count;
    int_hist_t items_per_category;

    std::vector<ItemInfo> items_available;
    std::vector<ItemInfo> items_ending_soon;
    std::vector<ItemInfo> items_waiting_for_purchase;
    std::vector<ItemInfo> items_completed;

    const std::vector<ItemInfo> all_item_sets[4] = {
        items_available,
        items_ending_soon,
        items_waiting_for_purchase,
        items_completed};

    std::vector<GlobalAttributeGroupId> gag_ids;

    std::optional<UserIdGenerator> user_id_generator;

    std::binomial_distribution<int> random_time_diff;
    std::binomial_distribution<int> random_duration;
    Zipf random_num_images;
    Zipf random_num_attributes;
    Zipf random_purchase_duration;
    Zipf random_num_comments;
    Zipf random_initial_price;

    std::optional<FlatHistogram<>> random_category;
    std::optional<FlatHistogram<>> random_item_count;

    std::chrono::system_clock::time_point last_close_auctions_time;
    std::chrono::system_clock::time_point client_start_time;
    std::chrono::system_clock::time_point current_time;

    str_cat_hist_t seller_item_cnt;

    std::vector<ItemCommentResponse> pending_comment_responses;

    // Temporary variables
    std::unordered_set<ItemInfo> tmp_seen_items;
    std::unordered_set<UserId> tmp_user_id_set;
    std::chrono::system_clock::time_point tmp_now;

    inline void initialize_user_id_generator(int client_id)
    {
      user_id_generator = UserIdGenerator(users_per_item_count, num_clients, client_id);
    }
  };

} // namespace auctionmark