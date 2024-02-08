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
  constexpr uint32_t NEW_USER_RATIO = 5;
  constexpr uint32_t NEW_ITEM_RATIO = 10;
  constexpr uint32_t NEW_BID_RATIO = 18;
  constexpr uint32_t NEW_COMMENT_RATIO = 2;
  constexpr uint32_t NEW_COMMENT_RESPONSE_RATIO = 1;
  constexpr uint32_t NEW_PURCHASE_RATIO = 2;
  constexpr uint32_t NEW_FEEDBACK_RATIO = 3;
  constexpr uint32_t GET_ITEM_RATIO = 40;
  constexpr uint32_t UPDATE_ITEM_RATIO = 2;
  constexpr uint32_t GET_COMMENT_RATIO = 2;
  constexpr uint32_t GET_USER_INFO_RATIO = 10;
  constexpr uint32_t GET_WATCHED_ITEMS_RATIO = 5;
}

#endif
