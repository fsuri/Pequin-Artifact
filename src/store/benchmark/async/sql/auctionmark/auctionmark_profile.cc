#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include <algorithm>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

namespace auctionmark
{

  AuctionMarkProfile *AuctionMarkProfile::cached_profile = nullptr;

  // AuctionMarkProfile::AuctionMarkProfile(){
  //    std::cerr << "Constructing empty AuctionMarkProfile. Don't use this" << std::endl;
  // }

  AuctionMarkProfile::AuctionMarkProfile(int client_id, int num_clients, double scale_factor) 
    : client_id(client_id), num_clients(num_clients), scale_factor(scale_factor), 
      random_time_diff(gen, 0, ITEM_DURATION_DAYS_MAX * 24 *60 *60 + ITEM_PRESERVE_DAYS*24*60*60), 
      random_duration(gen, ITEM_DURATION_DAYS_MIN, ITEM_DURATION_DAYS_MAX)
  {
    gen.seed(client_id);
    //std::cerr << "Constructing AuctionMarkProfile" << std::endl;
    struct timeval time;
    gettimeofday(&time, NULL);
    loader_start_time = get_ts(time);
    user_id_generator = std::nullopt;

    // TODO: Write getter methods to do the appropriate conversions for binomials
    //FIXME: Replace by gaussian...
    //random_time_diff = std::binomial_distribution<int>((ITEM_DURATION_DAYS_MAX * 24 * 60 * 60) - (ITEM_PRESERVE_DAYS * 24 * 60 * 60 * -1), 0.5);
    //random_time_diff = GaussGenerator(gen, ITEM_PRESERVE_DAYS*24*60*60*-1, ITEM_DURATION_DAYS_MAX * 24 *60 *60);
    //random_duration = std::binomial_distribution<int>(ITEM_DURATION_DAYS_MAX - ITEM_DURATION_DAYS_MIN, 0.5);
    //random_duration = GaussGenerator(gen, ITEM_DURATION_DAYS_MIN, ITEM_DURATION_DAYS_MAX);

    random_initial_price = Zipf(gen, ITEM_INITIAL_PRICE_MIN, ITEM_INITIAL_PRICE_MAX, ITEM_INITIAL_PRICE_SIGMA);
    random_purchase_duration = Zipf(gen, ITEM_PURCHASE_DURATION_DAYS_MIN, ITEM_PURCHASE_DURATION_DAYS_MAX, ITEM_PURCHASE_DURATION_DAYS_SIGMA);
    random_num_images = Zipf(gen, ITEM_NUM_IMAGES_MIN, ITEM_NUM_IMAGES_MAX, ITEM_NUM_IMAGES_SIGMA);
    random_num_attributes = Zipf(gen, ITEM_NUM_GLOBAL_ATTRS_MIN, ITEM_NUM_GLOBAL_ATTRS_MAX, ITEM_NUM_GLOBAL_ATTRS_SIGMA);
    random_num_comments = Zipf(gen, ITEM_NUM_COMMENTS_MIN, ITEM_NUM_COMMENTS_MAX, ITEM_NUM_COMMENTS_SIGMA);
  }

  // -----------------------------------------------------------------
  // TIME METHODS
  // -----------------------------------------------------------------

  uint64_t AuctionMarkProfile::get_scaled_current_timestamp(uint64_t& time)
  {
    struct timeval time_v;
    gettimeofday(&time_v, NULL);
    uint64_t tmp_now = get_ts(time_v);
    time = GetScaledTimestamp(loader_start_time, client_start_time, tmp_now);
    return time;
  }

  uint64_t AuctionMarkProfile::update_and_get_current_time()
  {
    current_time = get_scaled_current_timestamp(current_time);
    return current_time;
  }

  uint64_t AuctionMarkProfile::get_current_time()
  {
    return current_time;
  }

  uint64_t AuctionMarkProfile::get_loader_start_time()
  {
    return loader_start_time;
  }

  void AuctionMarkProfile::set_loader_start_time(uint64_t start_time) {
    loader_start_time = start_time;
  }

  uint64_t AuctionMarkProfile::get_loader_stop_time()
  {
    return loader_stop_time;
  }

  void AuctionMarkProfile::set_loader_stop_time(uint64_t stop_time) {
    loader_stop_time = stop_time;
  }

  uint64_t AuctionMarkProfile::set_and_get_client_start_time()
  {
    struct timeval time;
    gettimeofday(&time, NULL);
    client_start_time = get_ts(time);
    return client_start_time;
  }

  uint64_t AuctionMarkProfile::get_client_start_time()
  {
    return client_start_time;
  }

  bool AuctionMarkProfile::has_client_start_time()
  {
    return client_start_time > 0;
  }

  uint64_t AuctionMarkProfile::update_and_get_last_close_auctions_time()
  {
    last_close_auctions_time = get_scaled_current_timestamp(last_close_auctions_time);
    return last_close_auctions_time;
  }

  uint64_t AuctionMarkProfile::get_last_close_auctions_time()
  {
    return last_close_auctions_time;
  }

//TESTER variables
static int s = 0;
static int cnt = 0;
static int tot = 30000;
 
  int AuctionMarkProfile::get_random_time_diff()
  {
    
    //return random_time_diff(gen) + (ITEM_PRESERVE_DAYS * 24 * 60 * 60 * -1);
    int next = random_time_diff.next_val() + (ITEM_PRESERVE_DAYS*24*60*60*-1);
    //std::cerr << "chosen: " << next << std::endl;
    s += next;
    if(++cnt == tot){
      std::cerr << "sum: " << s << std::endl;
      std::cerr << "avg: " << (s/tot) << std::endl;
    }


    return next * MILLISECONDS_IN_A_SECOND; 
  }

  int AuctionMarkProfile::get_random_duration()
  {
    //return random_duration(gen) + ITEM_DURATION_DAYS_MIN;
    return random_duration.next_val(); 
  }

  // -----------------------------------------------------------------
  // GENERAL METHODS
  // -----------------------------------------------------------------

  double AuctionMarkProfile::get_scale_factor()
  {
    return scale_factor;
  }

  void AuctionMarkProfile::set_scale_factor(double scale_factor)
  {
    this->scale_factor = scale_factor;
  }

  // ----------------------------------------------------------------
  // USER METHODS
  // ----------------------------------------------------------------

  UserId AuctionMarkProfile::get_random_user_id(int min_item_count, int client_id, std::vector<UserId> &exclude)
  {
    if (!random_item_count.has_value())
    {
       //most users will have 0 items (between 0 and 1000, heavily skewed towards 0)
      // for(auto &[item, users]: users_per_item_count){
      //   std::cerr << "item_cnt: " << item << " --> " << users << std::endl;
      // }
      auto hist = FlatHistogram_Int(gen, users_per_item_count);  
      random_item_count.emplace(hist);
    }
    if (!user_id_generator.has_value()){
      initialize_user_id_generator(client_id);
    }

    std::optional<UserId> user_id = std::nullopt;
    int tries = 1000;
    int num_users = user_id_generator->get_total_users() - 1;


    while (!user_id.has_value() && tries-- > 0)
    {
      // We first need to figure out how many items our seller needs to have
      int item_count = -1;
      while (item_count < min_item_count)
      {
        auto hist = random_item_count.value();
        item_count = hist.next_value();
      }
     
      // Set the current item count and then choose a random position between where the generator is currently at and where it ends
      user_id_generator->set_current_item_count(item_count);
      int cur_position = user_id_generator->get_current_position();
      int new_position = std::uniform_int_distribution<>(cur_position, num_users)(gen);
      user_id = user_id_generator->seek_to_position(new_position);
      if (!user_id.has_value())
      {
        //std::cerr << "didn't find val" << std::endl;
        continue;
      }

      // Make sure that we didn't select the same UserId as the one we were
      // told to exclude.
      if (!exclude.empty()) {
        for (UserId ex : exclude) {
          if (ex == user_id.value()){
            //std::cerr << "val is meant to be excluded. skipping" << std::endl;
            user_id = std::nullopt;
            break;
          }
        }
        if (!user_id.has_value()){
          continue;
        }
      }

      // If we don't care about skew, then we're done right here
      break;
    }

    if (!user_id.has_value()) throw std::runtime_error("Failed to find a user_id after 1000 tries");
    
    return user_id.value();
  }

  UserId AuctionMarkProfile::get_random_buyer_id()
  {
    // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
    std::vector<UserId> exclude;
    return get_random_user_id(0, -1, exclude);
  }

  UserId AuctionMarkProfile::get_random_buyer_id(UserId exclude)
  {
    // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
    std::vector<UserId> exclude_vec = {exclude};
    return get_random_user_id(0, -1, exclude_vec);
  }

  UserId AuctionMarkProfile::get_random_buyer_id(std::vector<UserId> exclude)
  {
    // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
    return get_random_user_id(0, -1, exclude);
  }


  UserId AuctionMarkProfile::get_random_buyer_id(std::map<UserId, uint64_t> &previous_bidders, std::vector<UserId> exclude)
  {
    // This is very inefficient, but it's probably good enough for now
    
    std::map<UserId, uint64_t> tmp_hist = previous_bidders;
    for(auto &user: exclude){
      tmp_hist.erase(user);
    } 
    tmp_hist[get_random_buyer_id(exclude)]++;

    auto rand_h = FlatHistogram<UserId>(gen, tmp_hist);
    return rand_h.next_value();
  }

  UserId AuctionMarkProfile::get_random_seller_id(int client)
  {
    std::vector<UserId> exclude = {};
    return get_random_user_id(1, client, exclude);
  }

  void AuctionMarkProfile::add_pending_item_comment_response(ItemCommentResponse &cr)
  {
    if (client_id != -1)
    {
      const UserId seller_id(cr.get_seller_id());
      if (!user_id_generator->check_client(seller_id))
      {
        return;
      }
    }
    pending_comment_responses.push_back(cr);
  }

  // ----------------------------------------------------------------
  // ITEM METHODS
  // ----------------------------------------------------------------

    ItemId AuctionMarkProfile::get_next_item_id(UserId &seller_id)
  {
    std::string composite_id = seller_id.encode();
   
    int cnt = seller_item_cnt[composite_id];
    
    if (cnt == 0)
    {
      cnt = seller_id.get_item_count();
      // TODO: Test that this actually works.
      seller_item_cnt[composite_id] = cnt;
    }
    return ItemId(seller_id, cnt);
  }

  // ItemId AuctionMarkProfile::get_next_item_id(UserId &seller_id)
  // {
  //   std::string composite_id = seller_id.encode();
  //   int cat_idx = seller_item_cnt.axis(0).index(composite_id);
  //   int cnt = 0;
  //   try
  //   {
  //     int cnt = seller_item_cnt.at(cat_idx);
  //   }
  //   catch (std::out_of_range e)
  //   {
  //   }
  //   if (cnt == 0)
  //   {
  //     cnt = seller_id.get_item_count();
  //     // TODO: Test that this actually works.
  //     seller_item_cnt.at(cat_idx) = cnt;
  //   }
  //   return ItemId(seller_id, cnt);
  // }

  bool AuctionMarkProfile::add_item(std::vector<ItemInfo> &items, ItemInfo &item_info, bool is_loader)
  {
    bool added = false;

    auto it = std::find(items.begin(), items.end(), item_info);
    if (it != items.end())
    {
      *it = ItemInfo(item_info);
      return true;
    }

   
    if (item_info.has_current_price())
    {
      if (items.size() < ITEM_ID_CACHE_SIZE)
      {
        items.push_back(item_info);

        added = true;
      }
      else if (std::uniform_int_distribution<>(0, 1)(gen))
      {
        items.erase(items.begin());
        items.push_back(item_info);
        added = true;
      }
    }
   
    return added;
  }

  void AuctionMarkProfile::update_item_queues()
  {
    auto current_time = update_and_get_current_time();

    for (auto& items : all_item_sets)
    {
      if (*items == items_completed)
      {
        continue;
      }

      for (std::vector<ItemInfo>::iterator it = items->begin(); it != items->end(); it++)
      {
        std::pair p { it, items };
        auto current_queue_iterator = std::make_optional(p);
        ItemInfo item_info = *it;
        add_item_to_proper_queue(item_info, current_time, current_queue_iterator);
      }
    }
  }

  std::optional<ItemStatus> AuctionMarkProfile::add_item_to_proper_queue(ItemInfo &item_info, bool is_loader)
  {
    uint64_t base_time = is_loader ? get_loader_start_time() : get_current_time();
    return add_item_to_proper_queue(item_info, base_time, std::nullopt, is_loader);
  }

  std::optional<ItemStatus> AuctionMarkProfile::add_item_to_proper_queue(ItemInfo &item_info, uint64_t &base_time, std::optional<std::pair<std::vector<ItemInfo>::iterator, std::vector<ItemInfo>*>> current_queue_iterator,  bool is_loader)
  {
    if (client_id != -1)
    {
      if (user_id_generator == std::nullopt)
      {
        initialize_user_id_generator(client_id);
      }
      const UserId item_seller = item_info.get_seller_id();
      if (user_id_generator->check_client(item_seller))
      {
        return std::nullopt;
      }
    }

    uint64_t remaining = item_info.get_end_date() - base_time;
    //std::optional<ItemStatus> existing_status = item_info.get_status();
    ItemStatus new_status = item_info.get_status(); //(existing_status.has_value() ? existing_status.value() : ItemStatus::OPEN); //OPEN is now default.

    if (remaining <= 0)
    {
      new_status = ItemStatus::CLOSED;
    }
    else if (remaining < (ITEM_ENDING_SOON * MILLISECONDS_IN_A_SECOND))
    {
      new_status = ItemStatus::ENDING_SOON;
    }
    else if (item_info.get_num_bids() > 0 && new_status != ItemStatus::CLOSED)
    {
      new_status = ItemStatus::WAITING_FOR_PURCHASE;
    }

    if (new_status != item_info.get_status() || is_loader)
    //if (!existing_status.has_value() || new_status != existing_status.value())
    {
      if (current_queue_iterator.has_value())
      {
        current_queue_iterator->second->erase(current_queue_iterator->first);
      }

      switch (new_status)
      {
      case ItemStatus::OPEN:
        add_item(items_available, item_info);
        break;
      case ItemStatus::ENDING_SOON:
        add_item(items_ending_soon, item_info);
        break;
      case ItemStatus::WAITING_FOR_PURCHASE:
        add_item(items_waiting_for_purchase, item_info);
        break;
      case ItemStatus::CLOSED:
        add_item(items_completed, item_info);
        break;
      }
      item_info.set_status(new_status);
    }

    return new_status;
  }

  std::optional<ItemInfo> AuctionMarkProfile::get_random_item(std::vector<ItemInfo> &item_set, bool need_current_price, bool need_future_end_date)
  {
    auto current_time = update_and_get_current_time();
    int num_items = item_set.size();
    int idx = -1;
    std::optional<ItemInfo> item_info = std::nullopt;

    int tries = 1000;

    std::set<ItemInfo> tmp_seen_items;
    tmp_seen_items.clear();
    while (num_items > 0 && tries-- > 0 && tmp_seen_items.size() < num_items)
    {
      idx = std::uniform_int_distribution<>(0, num_items - 1)(gen);
      ItemInfo temp = item_set[idx];

      if (tmp_seen_items.count(temp))
      {
        continue;
      }
      tmp_seen_items.insert(temp);

      if (need_current_price && !temp.has_current_price())
      {
        continue;
      }

      if (need_future_end_date)
      {
        //auto temp_end_date = temp.get_end_date();
        bool compare_to = temp.has_end_date() ? temp.get_end_date() < current_time : true;
        if (compare_to)
        {
          continue;
        }
      }

      item_info = temp;
      break;
    }

    if (item_info != std::nullopt)
    {
      item_set.erase(item_set.begin() + idx);
      item_set.insert(item_set.begin(), item_info.value());
    }
    return item_info;
  }


  ItemId AuctionMarkProfile::processItemRecord(ItemRecord &row){
    assert(!row.sellerId.empty());
    if(row.itemStatus == ItemStatus::OPEN){

      ItemStatus i_status = ItemStatus::OPEN;
      ItemInfo itemInfo(row.itemId, row.currentPrice, row.endDate, (int) row.numBids);
      itemInfo.set_status(i_status);

      add_item_to_proper_queue(itemInfo, false);
    }
    return row.itemId;
  }

  /**********************************************************************************************
   * AVAILABLE ITEMS
   **********************************************************************************************/
  std::optional<ItemInfo> AuctionMarkProfile::get_random_available_item()
  {
    std::cerr << "items available: " << items_available.size() << std::endl;
    return get_random_item(items_available, false, false);
  }

  std::optional<ItemInfo> AuctionMarkProfile::get_random_available_item(bool has_current_price)
  {
    return get_random_item(items_available, has_current_price, false);
  }

  int AuctionMarkProfile::get_available_items_count()
  {
    return items_available.size();
  }

  /**********************************************************************************************
   * ENDING SOON ITEMS
   **********************************************************************************************/
  std::optional<ItemInfo> AuctionMarkProfile::get_random_ending_soon_item()
  {
    return get_random_item(items_ending_soon, false, false);
  }

  std::optional<ItemInfo> AuctionMarkProfile::get_random_ending_soon_item(bool has_current_price)
  {
    return get_random_item(items_ending_soon, has_current_price, false);
  }

  int AuctionMarkProfile::get_ending_soon_items_count()
  {
    return items_ending_soon.size();
  }

  /**********************************************************************************************
   * WAITING FOR PURCHASE ITEMS
   **********************************************************************************************/
  std::optional<ItemInfo> AuctionMarkProfile::get_random_waiting_for_purchase_item()
  {
    return get_random_item(items_waiting_for_purchase, false, false);
  }

  int AuctionMarkProfile::get_waiting_for_purchase_items_count()
  {
    return items_waiting_for_purchase.size();
  }

  /**********************************************************************************************
   * COMPLETED ITEMS
   **********************************************************************************************/
  std::optional<ItemInfo> AuctionMarkProfile::get_random_completed_item()
  {
    return get_random_item(items_completed, false, false);
  }

  int AuctionMarkProfile::get_completed_items_count()
  {
    return items_completed.size();
  }

  /**********************************************************************************************
   * ALL ITEMS
   **********************************************************************************************/
  std::optional<ItemInfo> AuctionMarkProfile::get_random_item()
  {
    int idx = -1;
    while (idx == -1 || all_item_sets[idx]->empty())
    {
      idx = std::uniform_int_distribution<>(0, 3)(gen);
    }
    return get_random_item(*all_item_sets[idx], false, false);
  }

  int AuctionMarkProfile::get_all_items_count()
  {
    return get_available_items_count() + get_ending_soon_items_count() + get_waiting_for_purchase_items_count() + get_completed_items_count();
  }

  // ----------------------------------------------------------------
  // GLOBAL ATTRIBUTE METHODS
  // ----------------------------------------------------------------
  GlobalAttributeValueId AuctionMarkProfile::get_random_global_attribute_value() {
    int offset = std::uniform_int_distribution<>(0, gag_ids.size() - 1)(gen);
    GlobalAttributeGroupId gag_id = gag_ids[offset];

    int count = std::uniform_int_distribution<>(0, gag_id.get_count())(gen);
    return GlobalAttributeValueId(gag_id, count);
  }

  int AuctionMarkProfile::get_random_category_id() {
    if (!random_category.has_value()) {
      //FlatHistogram_Int hist(gen, items_per_category);
      random_category.emplace(gen, items_per_category);
    }
    return random_category->next_value();
  }

  // -----------------------------------------------------------------
  // SERIALIZATION METHODS
  // -----------------------------------------------------------------
  void AuctionMarkProfile::save_profile() {
    std::cerr << "items_per_cat.size: " << items_per_category.size() << std::endl;
    std::ofstream profile_save_file;
    profile_save_file.open(PROFILE_FILE_NAME);
    {
      boost::archive::text_oarchive oa(profile_save_file);
      oa << scale_factor;
      oa << loader_start_time;
      oa << loader_stop_time;
      oa << users_per_item_count;
      oa << items_per_category;
      oa << gag_ids;
      oa << pending_comment_responses;
      oa << items_available;
      oa << items_waiting_for_purchase;
      oa << items_completed;
    }
    profile_save_file.close();
  }

  void AuctionMarkProfile::copy_profile(int client_id, const AuctionMarkProfile &other) {
    this->client_id = client_id;
    scale_factor = other.scale_factor;
    loader_start_time = other.loader_start_time;
    loader_stop_time = other.loader_stop_time;
    users_per_item_count = other.users_per_item_count;
    items_per_category = other.items_per_category;
    gag_ids = other.gag_ids;

    initialize_user_id_generator(client_id);

    for (int i = 0; i < ITEM_SETS_NUM; i++) {
      auto &list = *all_item_sets[i];
      auto &orig_list = *other.all_item_sets[i];

      for (ItemInfo& item_info : orig_list) {
        UserId seller_id = item_info.get_seller_id();
        if (user_id_generator->check_client(seller_id)) {
          seller_item_cnt.at(seller_id.encode()) = seller_id.get_item_count();
          list.push_back(item_info);
        }
      }
      std::shuffle(list.begin(), list.end(), gen);
    }

    for (ItemCommentResponse cr : other.pending_comment_responses) {
      UserId seller_id = UserId(cr.get_seller_id());
      if (user_id_generator->check_client(seller_id)) {
        pending_comment_responses.push_back(cr);
      }
    }
  }

  void AuctionMarkProfile::load_profile(const std::string &profile_file_path, int client_id) {
    if (AuctionMarkProfile::cached_profile == nullptr) {
      std::ifstream profile_save_file;
      profile_save_file.open(profile_file_path);
      {
        boost::archive::text_iarchive ia(profile_save_file);
        ia >> scale_factor;
        ia >> loader_start_time;
        ia >> loader_stop_time;
        ia >> users_per_item_count;
        ia >> items_per_category;
        ia >> gag_ids;
        ia >> pending_comment_responses;
        ia >> items_available;
        ia >> items_waiting_for_purchase;
        ia >> items_completed;
      }
      profile_save_file.close();

      AuctionMarkProfile::cached_profile = new AuctionMarkProfile(client_id, num_clients, scale_factor);
      AuctionMarkProfile::cached_profile->copy_profile(client_id, *this);
      AuctionMarkProfile::cached_profile->set_and_get_client_start_time();
      AuctionMarkProfile::cached_profile->update_and_get_current_time();
    } else {
      copy_profile(client_id, *AuctionMarkProfile::cached_profile);
    }
  }

  void AuctionMarkProfile::clear_cached_profile()
  {
    if (AuctionMarkProfile::cached_profile != nullptr)
    {
      delete AuctionMarkProfile::cached_profile;
      AuctionMarkProfile::cached_profile = nullptr;
    }
  }
} // namespace auctionmark