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

namespace auctionmark
{

  //FIXME: Copy this from AuctionMarkConstants

  //TABLE NAMES:

  const std::string TABLE_ITEM = "item";
  const std::string TABLE_ITEM_COLUMNS_STR = "item_columns_str";
  const std::string TABLE_ITEM_COMMENT = "item_comment";
  const std::string TABLE_ITEM_ATTR = "item_attr";
  const std::string TABLE_ITEM_IMAGE = "item_image";
  const std::string TABLE_ITEM_PURCHASE = "item_purchase";
  const std::string TABLE_ITEM_BID = "item_bid";
  const std::string TABLE_ITEM_MAX_BID = "item_max_bid";
  const std::string TABLE_ITEM_BID = "item_bid";

  const std::string TABLE_USER_ACCT = "user_acct";
  const std::string TABLE_USER_ACCT_ITEM = "user_acct_item";
  const std::string TABLE_USER_ACCT_FEEDBACK = "user_acct_feedback";
  const std::string TABLE_USER_ACCT_WATCH = "user_acct_watch";

  const std::string TABLE_REGION = "region";

  const std::string TABLE_CATEGORY = "category";
  const std::string TABLE_GLOBAL_ATTR_GROUP = "global_attr_group";
  const std::string TABLE_GLOBAL_ATTR_VALUE = "global_attr_value";

  const std::string ITEM_COLUMNS_STR = "i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status";

  const enum ItemStatus {
    OPEN,
    ENDING_SOON,
    WAITING_FOR_PURCHASE,
    CLOSED
  };


  ///////////////////

  constexpr uint32_t N_REGIONS = 75;
  constexpr uint32_t N_GAGS = 100;
  constexpr uint32_t GAV_PER_GROUP = 10;
  constexpr uint32_t N_USERS = 1000000;

  /** 1 sec in real time equals this value in the benchmark's virtual time in seconds */
  constexpr uint32_t TIME_SCALE_FACTOR = 600L; // one hour
  /**
  * If the amount of time in seconds remaining for an item auction is less than this parameter,
   * then it will be added to a special queue in the client. We will increase the likelihood that a
   * users will bid on these items as it gets closer to their end times
   */
  constexpr uint32_t ITEM_ENDING_SOON = 36000L; // 10 hours

  constexpr bool CLOSE_AUCTIONS_ENABLE = false;
  /**
   * How often to execute CLOSE_AUCTIONS in virtual seconds.
   */
  constexpr uint32_t CLOSE_AUCTIONS_INTERVAL = 12000L; // Every 20 seconds

  /* Transaction ratios, should add up to 100 */
  constexpr uint32_t TXNS_TOTAL = 100;
  
  constexpr uint32_t GET_ITEM_RATIO = 25;
  constexpr uint32_t GET_USER_INFO_RATIO = 15;
  constexpr uint32_t NEW_BID_RATIO = 20;
  constexpr uint32_t NEW_COMMENT_RATIO = 5;
  constexpr uint32_t NEW_COMMENT_RESPONSE_RATIO = 5;
  constexpr uint32_t NEW_FEEDBACK_RATIO = 5;
  constexpr uint32_t NEW_ITEM_RATIO = 10;
  constexpr uint32_t NEW_PURCHASE_RATIO = 5;
  constexpr uint32_t UPDATE_ITEM_RATIO = 10;


    /** When an item receives a bid we will increase its price by this amount */
  const double ITEM_BID_PERCENT_STEP = 0.025;


  const uint64_t SECONDS_IN_A_DAY = 24 * 60 * 60;
  const uint64_t MILLISECONDS_IN_A_SECOND = 1000;
  const uint64_t MILLISECONDS_IN_A_DAY = SECONDS_IN_A_DAY * MILLISECONDS_IN_A_SECOND;


  /** The number of update rounds in each invocation of CloseAuctions */
  const int CLOSE_AUCTIONS_ROUNDS = 1;
  /** The number of items to pull in for each update round in CloseAuctions */
  const int CLOSE_AUCTIONS_ITEMS_PER_ROUND = 100;
  
}

#endif
