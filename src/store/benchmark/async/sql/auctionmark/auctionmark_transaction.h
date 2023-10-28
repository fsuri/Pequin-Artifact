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
#ifndef AUCTIONMARK_TRANSACTION_H
#define AUCTIONMARK_TRANSACTION_H

#include "store/benchmark/async/sql/auctionmark/auctionmark_schema.h"
#include "store/common/frontend/sync_transaction.h"

namespace auctionmark {

template <class T>
void load_row(std::unique_ptr<query_result::Row> row, T& t) {
  row->get(0, &t);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          RegionRow& rr)
{
  uint32_t r_id;
  std::string r_name;
  row->get(0, &r_id);
  row->get(1, &r_name);
  rr = RegionRow(r_id, r_name);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          GlobalAttributeGroupRow& gag_row)
{
  uint32_t gag_id;
  uint32_t gag_c_id;
  std::string gag_name;
  row->get(0, &gag_id);
  row->get(1, &gag_c_id);
  row->get(2, &gag_name);
  gag_row = GlobalAttributeGroupRow(gag_id, gag_c_id, gag_name);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          GlobalAttributeValueRow& gav_row)
{
  uint32_t gav_id;
  uint32_t gav_gag_id;
  std::string gav_name;
  row->get(0, &gav_id);
  row->get(1, &gav_gag_id);
  row->get(2, &gav_name);
  gav_row = GlobalAttributeValueRow(gav_id, gav_gag_id, gav_name);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          CategoryRow& c_row)
{
  uint32_t c_id;
  std::string c_name;
  uint32_t c_parent_id;
  row->get(0, &c_id);
  row->get(1, &c_name);
  row->get(2, &c_parent_id);
  c_row = CategoryRow(c_id, c_name, c_parent_id);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          UserRow& u_row)
{
  uint32_t u_id;
  uint32_t u_rating;
  double u_balance;
  uint32_t u_created;
  uint32_t u_r_id;
  std::string u_sattr0;
  std::string u_sattr1;
  std::string u_sattr2;
  std::string u_sattr3;
  std::string u_sattr4;
  std::string u_sattr5;
  std::string u_sattr6;
  std::string u_sattr7;
  row->get(0, &u_id);
  row->get(1, &u_rating);
  row->get(2, &u_balance);
  row->get(3, &u_created);
  row->get(4, &u_r_id);
  row->get(5, &u_sattr0);
  row->get(6, &u_sattr1);
  row->get(7, &u_sattr2);
  row->get(8, &u_sattr3);
  row->get(9, &u_sattr4);
  row->get(10, &u_sattr5);
  row->get(11, &u_sattr6);
  row->get(12, &u_sattr7);
  u_row = UserRow(u_id, u_rating, u_balance, u_created, u_r_id, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, u_sattr5, u_sattr6, u_sattr7);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          UserAttributeRow& ua_row)
{
  uint32_t ua_id;
  uint32_t ua_u_id;
  std::string ua_name;
  std::string ua_value;
  uint32_t u_created;
  row->get(0, &ua_id);
  row->get(1, &ua_u_id);
  row->get(2, &ua_name);
  row->get(3, &ua_value);
  row->get(4, &u_created);
  ua_row = UserAttributeRow(ua_id, ua_u_id, ua_name, ua_value, u_created);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemRow& i_row)
{
  uint32_t i_id;
  uint32_t i_u_id;
  uint32_t i_c_id;
  std::string i_name;
  std::string i_description;
  std::string i_user_attributes;
  double i_initial_price;
  double i_current_price;
  uint32_t i_num_bids;
  uint32_t i_num_images;
  uint32_t i_num_global_attrs;
  uint32_t i_start_date;
  uint32_t i_end_date;
  uint32_t i_status;
  row->get(0, &i_id);
  row->get(1, &i_u_id);
  row->get(2, &i_c_id);
  row->get(3, &i_name);
  row->get(4, &i_description);
  row->get(5, &i_user_attributes);
  row->get(6, &i_initial_price);
  row->get(7, &i_current_price);
  row->get(8, &i_num_bids);
  row->get(9, &i_num_images);
  row->get(10, &i_num_global_attrs);
  row->get(11, &i_start_date);
  row->get(12, &i_end_date);
  row->get(13, &i_status);
  i_row = ItemRow(i_id, i_u_id, i_c_id, i_name, i_description, i_user_attributes, i_initial_price, i_current_price, i_num_bids, i_num_images, i_num_global_attrs, i_start_date, i_end_date, i_status);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemImageRow& ii_row)
{
  uint32_t ii_id;
  uint32_t ii_i_id;
  uint32_t ii_u_id;
  std::string ii_path;
  row->get(0, &ii_id);
  row->get(1, &ii_i_id);
  row->get(2, &ii_u_id);
  row->get(3, &ii_path);
  ii_row = ItemImageRow(ii_id, ii_i_id, ii_u_id, ii_path);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemCommentRow& ic_row)
{
  uint32_t ic_id;
  uint32_t ic_i_id;
  uint32_t ic_u_id;
  uint32_t ic_buyer_id;
  uint32_t ic_date;
  std::string ic_question;
  std::string ic_response;
  row->get(0, &ic_id);
  row->get(1, &ic_i_id);
  row->get(2, &ic_u_id);
  row->get(3, &ic_buyer_id);
  row->get(4, &ic_date);
  row->get(5, &ic_question);
  row->get(6, &ic_response);
  ic_row = ItemCommentRow(ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_date, ic_question, ic_response);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemFeedbackRow& if_row)
{
  uint32_t if_id;
  uint32_t if_i_id;
  uint32_t if_u_id;
  uint32_t if_buyer_id;
  uint32_t if_rating;
  uint32_t if_date;
  std::string if_comment;
  row->get(0, &if_id);
  row->get(1, &if_i_id);
  row->get(2, &if_u_id);
  row->get(3, &if_buyer_id);
  row->get(4, &if_rating);
  row->get(5, &if_date);
  row->get(6, &if_comment);
  if_row = ItemFeedbackRow(if_id, if_i_id, if_u_id, if_buyer_id, if_rating, if_date, if_comment);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemBidRow& ib_row)
{
  uint32_t ib_id;
  uint32_t ib_i_id;
  uint32_t ib_u_id;
  uint32_t ib_buyer_id;
  double ib_bid;
  double ib_max_bid;
  uint32_t ib_created;
  uint32_t ib_updated;
  row->get(0, &ib_id);
  row->get(1, &ib_i_id);
  row->get(2, &ib_u_id);
  row->get(3, &ib_buyer_id);
  row->get(4, &ib_bid);
  row->get(5, &ib_max_bid);
  row->get(6, &ib_created);
  row->get(7, &ib_updated);
  ib_row = ItemBidRow(ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemMaxBidRow& imb_row)
{
  uint32_t imb_i_id;
  uint32_t imb_u_id;
  uint32_t imb_ib_id;
  uint32_t imb_ib_i_id;
  uint32_t imb_ib_u_id;
  uint32_t imb_created;
  uint32_t imb_updated;
  row->get(0, &imb_i_id);
  row->get(1, &imb_u_id);
  row->get(2, &imb_ib_id);
  row->get(3, &imb_ib_i_id);
  row->get(4, &imb_ib_u_id);
  row->get(5, &imb_created);
  row->get(6, &imb_updated);
  imb_row = ItemMaxBidRow(imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemPurchaseRow& ip_row)
{
  uint32_t ip_id;
  uint32_t ip_ib_id;
  uint32_t ip_i_id;
  uint32_t ip_u_id;
  uint32_t ip_date;
  row->get(0, &ip_id);
  row->get(1, &ip_ib_id);
  row->get(2, &ip_i_id);
  row->get(3, &ip_u_id);
  row->get(4, &ip_date);
  ip_row = ItemPurchaseRow(ip_id, ip_ib_id, ip_i_id, ip_u_id, ip_date);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          UserItemRow& ui_row)
{
  uint32_t ui_u_id;
  uint32_t ui_i_id;
  uint32_t ui_seller_id;
  uint32_t ui_created;
  row->get(0, &ui_u_id);
  row->get(1, &ui_i_id);
  row->get(2, &ui_seller_id);
  row->get(3, &ui_created);
  ui_row = UserItemRow(ui_u_id, ui_i_id, ui_seller_id, ui_created);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  load_row(queryResult->at(row), t);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult) {
  deserialize(t, queryResult, 0);
}

class AuctionMarkTransaction : public SyncTransaction {
 public:
  AuctionMarkTransaction(uint32_t timeout);
  virtual ~AuctionMarkTransaction();
};

}

#endif /* AUCTIONMARK_TRANSACTION_H */
