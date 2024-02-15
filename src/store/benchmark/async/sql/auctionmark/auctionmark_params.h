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
#ifndef AUCTIONMARK_PARAMS_H
#define AUCTIONMARK_PARAMS_H

#include <stdint.h>
#include <unordered_set>

namespace auctionmark
{

  const double SCALE_FACTOR = 1;

  static constexpr uint32_t N_REGIONS = 75;
  static constexpr uint32_t N_GAGS = 100;
  static constexpr uint32_t GAV_PER_GROUP = 10;
  static constexpr uint32_t N_USERS = 1000000;

  /* Time Parameters */
  static constexpr long SECONDS_IN_A_DAY = 24 * 60 * 60;
  static constexpr long MILLISECONDS_IN_A_SECOND = 1000;
  static constexpr long MILLISECONDS_IN_A_DAY = SECONDS_IN_A_DAY * MILLISECONDS_IN_A_SECOND;

  /** 1 sec in real time equals this value in the benchmark's virtual time in seconds */
  static constexpr uint32_t TIME_SCALE_FACTOR = 600L; // one hour
  /**
  * If the amount of time in seconds remaining for an item auction is less than this parameter,
   * then it will be added to a special queue in the client. We will increase the likelihood that a
   * users will bid on these items as it gets closer to their end times
   */
  static constexpr uint32_t ITEM_ENDING_SOON = 36000L; // 10 hours
  static constexpr int ITEM_ALREADY_ENDED = 100000;

  /* Execution Configuration */
  static constexpr bool CLOSE_AUCTIONS_ENABLE = false;

  /**
   * How often to execute CLOSE_AUCTIONS in virtual seconds.
   */
  static constexpr uint32_t CLOSE_AUCTIONS_INTERVAL = 12000L; // Every 20 seconds

  /**
   * If set to true, the CloseAuctions transactions will be a executed in a separate thread. If set
   * to false, then these txns will be executed whenever the interval interrupt occurs on the first
   * worker thread
   */
  static constexpr bool CLOSE_AUCTIONS_SEPARATE_THREAD = false;

  /**
   * If set to true, then the first client will attempt to reset the database before starting the
   * benchmark execution
   */
  static constexpr bool RESET_DATABASE_ENABLE = false;


  /* Transaction ratios, should add up to 100 */
  static constexpr int TXNS_TOTAL = 100;
  static constexpr int FREQUENCY_GET_ITEM = 25;
  static constexpr int FREQUENCY_GET_USER_INFO = 15;
  static constexpr int FREQUENCY_NEW_BID = 20;
  static constexpr int FREQUENCY_NEW_COMMENT = 5;
  static constexpr int FREQUENCY_NEW_COMMENT_RESPONSE = 5;
  static constexpr int FREQUENCY_NEW_FEEDBACK = 5;
  static constexpr int FREQUENCY_NEW_ITEM = 10;
  static constexpr int FREQUENCY_NEW_PURCHASE = 5;
  static constexpr int FREQUENCY_UPDATE_ITEM = 10;

  // Non-standard txns
  static constexpr int FREQUENCY_CLOSE_AUCTIONS = -1; // called at regular intervals

  /* Default Table Sizes*/
  static constexpr long TABLESIZE_REGION = 75;
  static constexpr long TABLESIZE_GLOBAL_ATTRIBUTE_GROUP = 100;
  static constexpr long TABLESIZE_GLOBAL_ATTRIBUTE_VALUE = 1; // HACK: IGNORE
  static constexpr long TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP = 10;
  static constexpr long TABLESIZE_USERACCT = 10000;

  /* User parameters */
  static constexpr int USER_MIN_ATTRIBUTES = 0;
  static constexpr int USER_MAX_ATTRIBUTES = 5;

  static constexpr long USER_MIN_BALANCE = 1000;
  static constexpr long USER_MAX_BALANCE = 100000;

  static constexpr long USER_MIN_RATING = 0;
  static constexpr long USER_MAX_RATING = 10000;

  static constexpr int USER_ATTRIBUTE_NAME_LENGTH_MIN = 5;
  static constexpr int USER_ATTRIBUTE_NAME_LENGTH_MAX = 32;

  static constexpr int USER_ATTRIBUTE_VALUE_LENGTH_MIN = 5;
  static constexpr int USER_ATTRIBUTE_VALUE_LENGTH_MAX = 32;

  /* Item parameters */
  static constexpr uint32_t ITEM_INITIAL_PRICE_MIN = 1;
  static constexpr uint32_t ITEM_INITIAL_PRICE_MAX = 1000;
  static constexpr uint32_t ITEM_INITIAL_PRICE_SIGMA = 1.25;
  static constexpr int ITEM_ITEMS_PER_SELLER_MIN = 0;
  static constexpr int ITEM_ITEMS_PER_SELLER_MAX = 1000;
  static constexpr double ITEM_ITEMS_PER_SELLER_SIGMA = 2.0;

  static constexpr int ITEM_BIDS_PER_DAY_MIN = 0;
  static constexpr int ITEM_BIDS_PER_DAY_MAX = 10;
  static constexpr double ITEM_BIDS_PER_DAY_SIGMA = 1.25;

  static constexpr int ITEM_WATCHES_PER_DAY_MIN = 0;
  static constexpr int ITEM_WATCHES_PER_DAY_MAX = 5;
  static constexpr double ITEM_WATCHES_PER_DAY_SIGMA = 1.25;

  static constexpr int ITEM_NUM_IMAGES_MIN = 1;
  static constexpr int ITEM_NUM_IMAGES_MAX = 10;
  static constexpr double ITEM_NUM_IMAGES_SIGMA = 1.25;

  static constexpr int ITEM_NUM_COMMENTS_MIN = 0;
  static constexpr int ITEM_NUM_COMMENTS_MAX = 5;
  static constexpr double ITEM_NUM_COMMENTS_SIGMA = 1.25;

  static constexpr int ITEM_COMMENT_LENGTH_MIN = 10;
  static constexpr int ITEM_COMMENT_LENGTH_MAX = 128;

  static constexpr int ITEM_NUM_GLOBAL_ATTRS_MIN = 1;
  static constexpr int ITEM_NUM_GLOBAL_ATTRS_MAX = 10;
  static constexpr double ITEM_NUM_GLOBAL_ATTRS_SIGMA = 1.25;

  static constexpr int ITEM_NAME_LENGTH_MIN = 16;
  static constexpr int ITEM_NAME_LENGTH_MAX = 100;

  static constexpr int ITEM_DESCRIPTION_LENGTH_MIN = 50;
  static constexpr int ITEM_DESCRIPTION_LENGTH_MAX = 255;

  static constexpr int ITEM_USER_ATTRIBUTES_LENGTH_MIN = 20;
  static constexpr int ITEM_USER_ATTRIBUTES_LENGTH_MAX = 255;

  static constexpr float ITEM_BID_PERCENT_STEP = 0.025f;     /** When an item receives a bid we will increase its price by this amount */

  static constexpr int ITEM_PURCHASE_DURATION_DAYS_MIN = 0;
  static constexpr int ITEM_PURCHASE_DURATION_DAYS_MAX = 7;
  static constexpr double ITEM_PURCHASE_DURATION_DAYS_SIGMA = 1.1;

  static constexpr int ITEM_PRESERVE_DAYS = 7;

  static constexpr int ITEM_DURATION_DAYS_MIN = 1;
  static constexpr int ITEM_DURATION_DAYS_MAX = 10;

  static constexpr int ITEM_LOADCONFIG_LIMIT = 5000;

  static constexpr int ITEM_ID_CACHE_SIZE = 1000;
 
  static constexpr int CLOSE_AUCTIONS_ROUNDS = 1;  /** The number of update rounds in each invocation of CloseAuctions */

  static constexpr int CLOSE_AUCTIONS_ITEMS_PER_ROUND = 100;  /** The number of items to pull in for each update round in CloseAuctions */

  static constexpr const char* ITEM_COLUMNS[7] = {
        "i_id", "i_u_id", "i_name", "i_current_price", "i_num_bids", "i_end_date", "i_status"
    };

  static const std::string ITEM_COLUMNS_STR = "i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status";

  static constexpr const int ITEM_SETS_NUM = 4;

  /* Table Names */
  static constexpr const char* TABLE_REGION = "region";
  static constexpr const char* TABLE_USERACCT = "useracct";
  //static constexpr const char* TABLE_USERACCT_ATTRIBUTES = "useracct_attributes";  //NO TX USES THIS
  static constexpr const char* TABLE_USERACCT_ITEM = "useracct_item";
  static constexpr const char* TABLE_USERACCT_WATCH = "useracct_watch";
  static constexpr const char* TABLE_USERACCT_FEEDBACK = "useracct_feedback";
  static constexpr const char* TABLE_CATEGORY = "category";
  static constexpr const char* TABLE_GLOBAL_ATTR_GROUP = "global_attribute_group";
  static constexpr const char* TABLE_GLOBAL_ATTR_VALUE = "global_attribute_value";
  static constexpr const char* TABLE_ITEM = "item";
  static constexpr const char* TABLE_ITEM_ATTR = "item_attribute";
  static constexpr const char* TABLE_ITEM_IMAGE = "item_image";
  static constexpr const char* TABLE_ITEM_COMMENT = "item_comment";
  static constexpr const char* TABLE_ITEM_BID = "item_bid";
  static constexpr const char* TABLE_ITEM_MAX_BID = "item_max_bid";
  static constexpr const char* TABLE_ITEM_PURCHASE = "item_purchase";

  static constexpr const char* TABLENAMES[15] = {
      TABLE_REGION,
      TABLE_CATEGORY,
      TABLE_GLOBAL_ATTR_GROUP,
      TABLE_GLOBAL_ATTR_VALUE,
      TABLE_USERACCT,
      //TABLE_USERACCT_ATTRIBUTES,
      TABLE_USERACCT_ITEM,
      TABLE_USERACCT_WATCH,
      TABLE_USERACCT_FEEDBACK,
      TABLE_ITEM,
      TABLE_ITEM_ATTR,
      TABLE_ITEM_IMAGE,
      TABLE_ITEM_COMMENT,
      TABLE_ITEM_BID,
      TABLE_ITEM_MAX_BID,
      TABLE_ITEM_PURCHASE,
  };

  /* Table Data Structures */
  static const std::unordered_set<std::string> FIXED_TABLES = {
    TABLE_REGION,
    TABLE_GLOBAL_ATTR_GROUP,
    TABLE_GLOBAL_ATTR_VALUE
  };

  static const std::unordered_set<std::string> DYNAMIC_TABLES = {
    //TABLE_USERACCT_ATTRIBUTES,
    TABLE_ITEM_IMAGE,
    TABLE_ITEM_ATTR,
    TABLE_ITEM_COMMENT,
    TABLE_USERACCT_FEEDBACK,
    TABLE_ITEM_BID,
    TABLE_ITEM_MAX_BID,
    TABLE_ITEM_PURCHASE,
    TABLE_USERACCT_ITEM,
    TABLE_USERACCT_WATCH
  };

  static const std::unordered_set<std::string> DATAFILE_TABLES = {
    TABLE_CATEGORY
  };

  /* Probabilities */

  /** The probability that a buyer will leave feedback for the seller (1-100) */
  static constexpr int PROB_PURCHASE_BUYER_LEAVES_FEEDBACK = 75;

  /** The probability that a seller will leave feedback for the buyer (1-100) */
  static constexpr int PROB_PURCHASE_SELLER_LEAVES_FEEDBACK = 80;

  static constexpr int PROB_GETUSERINFO_INCLUDE_FEEDBACK = 25;
  static constexpr int PROB_GETUSERINFO_INCLUDE_COMMENTS = 10;
  static constexpr int PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS = 10;
  static constexpr int PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS = 10;
  static constexpr int PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS = 10;

  static constexpr int PROB_UPDATEITEM_DELETE_ATTRIBUTE = 25;
  static constexpr int PROB_UPDATEITEM_ADD_ATTRIBUTE = -1; // 25;

  /** The probability that a buyer will not have enough money to purchase an item (1-100) */
  static constexpr int PROB_NEWPURCHASE_NOT_ENOUGH_MONEY = 1;

  /** The probability that the NewBid txn will try to bid on a closed item (1-100) */
  static constexpr int PROB_NEWBID_CLOSED_ITEM = 5;

  /** The probability that a NewBid txn will target an item whose auction is ending soon (1-100) */
  static constexpr int PROB_NEWBID_ENDINGSOON_ITEM = 50;

  /* Profile */
  static const std::string PROFILE_FILE_NAME = "auctionmark_profile.csv";
  static const std::string PROFILE_HIST_SAVE_FILE_NAME = "auctionmark_profile_hist.csv";
} 
  

#endif
