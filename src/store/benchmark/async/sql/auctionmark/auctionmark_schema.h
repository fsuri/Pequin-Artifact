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
#ifndef AUCTIONMARK_SCHEMA_H
#define AUCTIONMARK_SCHEMA_H

#include <stdint.h>
#include <string>

namespace auctionmark {

   //TODO: Clear this file.

class RegionRow {
 private:
    uint32_t r_id;
    std::string r_name;

 public:
    RegionRow() {}
    RegionRow(uint32_t r_id, std::string r_name) : r_id(r_id), r_name(r_name) {}

    auto get_r_id() -> uint32_t  { return r_id; }
    auto get_r_name() -> std::string&  { return r_name; }

    void set_r_id(uint32_t r_id) { this->r_id = r_id; }
    void set_r_name(std::string r_name) { this->r_name = r_name; }
};

class GlobalAttributeGroupRow {
 private:
    uint32_t gag_id;
    uint32_t gag_c_id;
    std::string gag_name;

 public:
    GlobalAttributeGroupRow() {}
    GlobalAttributeGroupRow(uint32_t gag_id, uint32_t gag_c_id, std::string gag_name) : gag_id(gag_id), gag_c_id(gag_c_id), gag_name(gag_name) {}

    auto get_gag_id() -> uint32_t  { return gag_id; }
    auto get_gag_c_id() -> uint32_t  { return gag_c_id; }
    auto get_gag_name() -> std::string&  { return gag_name; }

    void set_gag_id(uint32_t g_id) { this->gag_id = gag_id; }
    void set_gag_c_id(uint32_t g_c_id) { this->gag_c_id = gag_c_id; }
    void set_gag_name(std::string g_name) { this->gag_name = gag_name; }
};

class GlobalAttributeValueRow {
 private:
    uint32_t gav_id;
    uint32_t gav_gag_id;
    std::string gav_name;

 public:
    GlobalAttributeValueRow() {}
    GlobalAttributeValueRow(uint32_t gav_id, uint32_t gav_gag_id, std::string gav_name) : gav_id(gav_id), gav_gag_id(gav_gag_id), gav_name(gav_name) {}

    auto get_gav_id() -> uint32_t  { return gav_id; }
    auto get_gav_gag_id() -> uint32_t  { return gav_gag_id; }
    auto get_gav_name() -> std::string&  { return gav_name; }

    void set_gav_id(uint32_t gav_id) { this->gav_id = gav_id; }
    void set_gav_gag_id(uint32_t gav_gag_id) { this->gav_gag_id = gav_gag_id; }
    void set_gav_name(std::string gav_name) { this->gav_name = gav_name; }
};

class CategoryRow {
 private:
    uint32_t c_id;
    std::string c_name;
    uint32_t c_parent_id;

 public:
    CategoryRow() {}
    CategoryRow(uint32_t c_id, std::string c_name, uint32_t c_parent_id) : c_id(c_id), c_name(c_name), c_parent_id(c_parent_id) {}

    auto get_c_id() -> uint32_t  { return c_id; }
    auto get_c_name() -> std::string&  { return c_name; }
    auto get_c_parent_id() -> uint32_t  { return c_parent_id; }

    void set_c_id(uint32_t c_id) { this->c_id = c_id; }
    void set_c_name(std::string c_name) { this->c_name = c_name; }
    void set_c_parent_id(uint32_t c_parent_id) { this->c_parent_id = c_parent_id; }
};

class UserRow {
 private:
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

 public:
    UserRow() {}
    UserRow(uint32_t u_id, uint32_t u_rating, double u_balance, uint32_t u_created, uint32_t u_r_id, std::string u_sattr0, std::string u_sattr1, std::string u_sattr2, std::string u_sattr3, std::string u_sattr4, std::string u_sattr5, std::string u_sattr6, std::string u_sattr7) : u_id(u_id), u_rating(u_rating), u_balance(u_balance), u_created(u_created), u_r_id(u_r_id), u_sattr0(u_sattr0), u_sattr1(u_sattr1), u_sattr2(u_sattr2), u_sattr3(u_sattr3), u_sattr4(u_sattr4), u_sattr5(u_sattr5), u_sattr6(u_sattr6), u_sattr7(u_sattr7) {}

    auto get_u_id() -> uint32_t  { return u_id; }
    auto get_u_rating() -> uint32_t  { return u_rating; }
    auto get_u_balance() -> double  { return u_balance; }
    auto get_u_created() -> uint32_t  { return u_created; }
    auto get_u_r_id() -> uint32_t  { return u_r_id; }
    auto get_u_sattr0() -> std::string&  { return u_sattr0; }
    auto get_u_sattr1() -> std::string&  { return u_sattr1; }
    auto get_u_sattr2() -> std::string&  { return u_sattr2; }
    auto get_u_sattr3() -> std::string&  { return u_sattr3; }
    auto get_u_sattr4() -> std::string&  { return u_sattr4; }
    auto get_u_sattr5() -> std::string&  { return u_sattr5; }
    auto get_u_sattr6() -> std::string&  { return u_sattr6; }
    auto get_u_sattr7() -> std::string&  { return u_sattr7; }

    void set_u_id(uint32_t u_id) { this->u_id = u_id; }
    void set_u_rating(uint32_t u_rating) { this->u_rating = u_rating; }
    void set_u_balance(double u_balance) { this->u_balance = u_balance; }
    void set_u_created(uint32_t u_created) { this->u_created = u_created; }
    void set_u_r_id(uint32_t u_r_id) { this->u_r_id = u_r_id; }
    void set_u_sattr0(std::string u_sattr0) { this->u_sattr0 = u_sattr0; }
    void set_u_sattr1(std::string u_sattr1) { this->u_sattr1 = u_sattr1; }
    void set_u_sattr2(std::string u_sattr2) { this->u_sattr2 = u_sattr2; }
    void set_u_sattr3(std::string u_sattr3) { this->u_sattr3 = u_sattr3; }
    void set_u_sattr4(std::string u_sattr4) { this->u_sattr4 = u_sattr4; }
    void set_u_sattr5(std::string u_sattr5) { this->u_sattr5 = u_sattr5; }
    void set_u_sattr6(std::string u_sattr6) { this->u_sattr6 = u_sattr6; }
    void set_u_sattr7(std::string u_sattr7) { this->u_sattr7 = u_sattr7; }
};

class UserAttributeRow {
 private:
    uint32_t ua_id;
    uint32_t ua_u_id;
    std::string ua_name;
    std::string ua_value;
    uint32_t u_created;

 public:
    UserAttributeRow() {}
    UserAttributeRow(uint32_t ua_id, uint32_t ua_u_id, std::string ua_name, std::string ua_value, uint32_t u_created) : ua_id(ua_id), ua_u_id(ua_u_id), ua_name(ua_name), ua_value(ua_value), u_created(u_created) {}

    auto get_ua_id() -> uint32_t  { return ua_id; }
    auto get_ua_u_id() -> uint32_t  { return ua_u_id; }
    auto get_ua_name() -> std::string&  { return ua_name; }
    auto get_ua_value() -> std::string&  { return ua_value; }
    auto get_u_created() -> uint32_t  { return u_created; }

    void set_ua_id(uint32_t ua_id) { this->ua_id = ua_id; }
    void set_ua_u_id(uint32_t ua_u_id) { this->ua_u_id = ua_u_id; }
    void set_ua_name(std::string ua_name) { this->ua_name = ua_name; }
    void set_ua_value(std::string ua_value) { this->ua_value = ua_value; }
    void set_u_created(uint32_t u_created) { this->u_created = u_created; }
};

class ItemRow {
 private:
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

 public:
    ItemRow() {}
    ItemRow(uint32_t i_id, uint32_t i_u_id, uint32_t i_c_id, std::string i_name, std::string i_description, std::string i_user_attributes, double i_initial_price, double i_current_price, uint32_t i_num_bids, uint32_t i_num_images, uint32_t i_num_global_attrs, uint32_t i_start_date, uint32_t i_end_date, uint32_t i_status) : i_id(i_id), i_u_id(i_u_id), i_c_id(i_c_id), i_name(i_name), i_description(i_description), i_user_attributes(i_user_attributes), i_initial_price(i_initial_price), i_current_price(i_current_price), i_num_bids(i_num_bids), i_num_images(i_num_images), i_num_global_attrs(i_num_global_attrs), i_start_date(i_start_date), i_end_date(i_end_date), i_status(i_status) {}

    auto get_i_id() -> uint32_t  { return i_id; }
    auto get_i_u_id() -> uint32_t  { return i_u_id; }
    auto get_i_c_id() -> uint32_t  { return i_c_id; }
    auto get_i_name() -> std::string&  { return i_name; }
    auto get_i_description() -> std::string&  { return i_description; }
    auto get_i_user_attributes() -> std::string&  { return i_user_attributes; }
    auto get_i_initial_price() -> double  { return i_initial_price; }
    auto get_i_current_price() -> double  { return i_current_price; }
    auto get_i_num_bids() -> uint32_t  { return i_num_bids; }
    auto get_i_num_images() -> uint32_t  { return i_num_images; }
    auto get_i_num_global_attrs() -> uint32_t  { return i_num_global_attrs; }
    auto get_i_start_date() -> uint32_t  { return i_start_date; }
    auto get_i_end_date() -> uint32_t  { return i_end_date; }
    auto get_i_status() -> uint32_t  { return i_status; }

    void set_i_id(uint32_t i_id) { this->i_id = i_id; }
    void set_i_u_id(uint32_t i_u_id) { this->i_u_id = i_u_id; }
    void set_i_c_id(uint32_t i_c_id) { this->i_c_id = i_c_id; }
    void set_i_name(std::string i_name) { this->i_name = i_name; }
    void set_i_description(std::string i_description) { this->i_description = i_description; }
    void set_i_user_attributes(std::string i_user_attributes) { this->i_user_attributes = i_user_attributes; }
    void set_i_initial_price(double i_initial_price) { this->i_initial_price = i_initial_price; }
    void set_i_current_price(double i_current_price) { this->i_current_price = i_current_price; }
    void set_i_num_bids(uint32_t i_num_bids) { this->i_num_bids = i_num_bids; }
    void set_i_num_images(uint32_t i_num_images) { this->i_num_images = i_num_images; }
    void set_i_num_global_attrs(uint32_t i_num_global_attrs) { this->i_num_global_attrs = i_num_global_attrs; }
    void set_i_start_date(uint32_t i_start_date) { this->i_start_date = i_start_date; }
    void set_i_end_date(uint32_t i_end_date) { this->i_end_date = i_end_date; }
    void set_i_status(uint32_t i_status) { this->i_status = i_status; }
};

class ItemImageRow {
 private:
    uint32_t ii_id;
    uint32_t ii_i_id;
    uint32_t ii_u_id;
    std::string ii_path;

 public:
    ItemImageRow() {}
    ItemImageRow(uint32_t ii_id, uint32_t ii_i_id, uint32_t ii_u_id, std::string ii_path) : ii_id(ii_id), ii_i_id(ii_i_id), ii_u_id(ii_u_id), ii_path(ii_path) {}

    auto get_ii_id() -> uint32_t  { return ii_id; }
    auto get_ii_i_id() -> uint32_t  { return ii_i_id; }
    auto get_ii_u_id() -> uint32_t  { return ii_u_id; }
    auto get_ii_path() -> std::string&  { return ii_path; }

    void set_ii_id(uint32_t ii_id) { this->ii_id = ii_id; }
    void set_ii_i_id(uint32_t ii_i_id) { this->ii_i_id = ii_i_id; }
    void set_ii_u_id(uint32_t ii_u_id) { this->ii_u_id = ii_u_id; }
    void set_ii_path(std::string ii_path) { this->ii_path = ii_path; }
};

class ItemCommentRow {
 private:
    uint32_t ic_id;
    uint32_t ic_i_id;
    uint32_t ic_u_id;
    uint32_t ic_buyer_id;
    std::string ic_question;
    uint64_t ic_created;

 public:
    ItemCommentRow() {}
    ItemCommentRow(uint32_t ic_id, uint32_t ic_i_id, uint32_t ic_u_id, uint32_t ic_buyer_id, std::string ic_question, uint64_t ic_created) : ic_id(ic_id), ic_i_id(ic_i_id), ic_u_id(ic_u_id), ic_buyer_id(ic_buyer_id), ic_date(ic_date), ic_question(ic_question), ic_response(ic_response) {}

    auto get_ic_id() -> uint32_t  { return ic_id; }
    auto get_ic_i_id() -> uint32_t  { return ic_i_id; }
    auto get_ic_u_id() -> uint32_t  { return ic_u_id; }
    auto get_ic_buyer_id() -> uint32_t  { return ic_buyer_id; }
    auto get_ic_created() -> uint64_t  { return ic_created; }
    auto get_ic_question() -> std::string&  { return ic_question; }
    

    void set_ic_id(uint32_t ic_id) { this->ic_id = ic_id; }
    void set_ic_i_id(uint32_t ic_i_id) { this->ic_i_id = ic_i_id; }
    void set_ic_u_id(uint32_t ic_u_id) { this->ic_u_id = ic_u_id; }
    void set_ic_buyer_id(uint32_t ic_buyer_id) { this->ic_buyer_id = ic_buyer_id; }
    void set_ic_date(uint64_t ic_date) { this->ic_created = ic_date; }
    void set_ic_question(std::string ic_question) { this->ic_question = ic_question; }
}
   

class ItemFeedbackRow {
 private:
    uint32_t if_id;
    uint32_t if_i_id;
    uint32_t if_u_id;
    uint32_t if_buyer_id;
    uint32_t if_rating;
    uint32_t if_date;
    std::string if_comment;

 public:
    ItemFeedbackRow() {}
    ItemFeedbackRow(uint32_t if_id, uint32_t if_i_id, uint32_t if_u_id, uint32_t if_buyer_id, uint32_t if_rating, uint32_t if_date, std::string if_comment) : if_id(if_id), if_i_id(if_i_id), if_u_id(if_u_id), if_buyer_id(if_buyer_id), if_rating(if_rating), if_date(if_date), if_comment(if_comment) {}

    auto get_if_id() -> uint32_t  { return if_id; }
    auto get_if_i_id() -> uint32_t  { return if_i_id; }
    auto get_if_u_id() -> uint32_t  { return if_u_id; }
    auto get_if_buyer_id() -> uint32_t  { return if_buyer_id; }
    auto get_if_rating() -> uint32_t  { return if_rating; }
    auto get_if_date() -> uint32_t  { return if_date; }
    auto get_if_comment() -> std::string&  { return if_comment; }

    void set_if_id(uint32_t if_id) { this->if_id = if_id; }
    void set_if_i_id(uint32_t if_i_id) { this->if_i_id = if_i_id; }
    void set_if_u_id(uint32_t if_u_id) { this->if_u_id = if_u_id; }
    void set_if_buyer_id(uint32_t if_buyer_id) { this->if_buyer_id = if_buyer_id; }
    void set_if_rating(uint32_t if_rating) { this->if_rating = if_rating; }
    void set_if_date(uint32_t if_date) { this->if_date = if_date; }
    void set_if_comment(std::string if_comment) { this->if_comment = if_comment; }
};

class ItemBidRow {
 private:
    uint32_t ib_id;
    uint32_t ib_i_id;
    uint32_t ib_u_id;
    uint32_t ib_buyer_id;
    double ib_bid;
    double ib_max_bid;
    uint32_t ib_created;
    uint32_t ib_updated;

 public:
    ItemBidRow() {}
    ItemBidRow(uint32_t ib_id, uint32_t ib_i_id, uint32_t ib_u_id, uint32_t ib_buyer_id, double ib_bid, double ib_max_bid, uint32_t ib_created, uint32_t ib_updated) : ib_id(ib_id), ib_i_id(ib_i_id), ib_u_id(ib_u_id), ib_buyer_id(ib_buyer_id), ib_bid(ib_bid), ib_max_bid(ib_max_bid), ib_created(ib_created), ib_updated(ib_updated) {}

    auto get_ib_id() -> uint32_t  { return ib_id; }
    auto get_ib_i_id() -> uint32_t  { return ib_i_id; }
    auto get_ib_u_id() -> uint32_t  { return ib_u_id; }
    auto get_ib_buyer_id() -> uint32_t  { return ib_buyer_id; }
    auto get_ib_bid() -> double  { return ib_bid; }
    auto get_ib_max_bid() -> double  { return ib_max_bid; }
    auto get_ib_created() -> uint32_t  { return ib_created; }
    auto get_ib_updated() -> uint32_t  { return ib_updated; }

    void set_ib_id(uint32_t ib_id) { this->ib_id = ib_id; }
    void set_ib_i_id(uint32_t ib_i_id) { this->ib_i_id = ib_i_id; }
    void set_ib_u_id(uint32_t ib_u_id) { this->ib_u_id = ib_u_id; }
    void set_ib_buyer_id(uint32_t ib_buyer_id) { this->ib_buyer_id = ib_buyer_id; }
    void set_ib_bid(double ib_bid) { this->ib_bid = ib_bid; }
    void set_ib_max_bid(double ib_max_bid) { this->ib_max_bid = ib_max_bid; }
    void set_ib_created(uint32_t ib_created) { this->ib_created = ib_created; }
    void set_ib_updated(uint32_t ib_updated) { this->ib_updated = ib_updated; }
};

class ItemMaxBidRow {
 private:
    uint32_t imb_i_id;
    uint32_t imb_u_id;
    uint32_t imb_ib_id;
    uint32_t imb_ib_i_id;
    uint32_t imb_ib_u_id;
    uint32_t imb_created;
    uint32_t imb_updated;

 public:
    ItemMaxBidRow() {}
    ItemMaxBidRow(uint32_t imb_i_id, uint32_t imb_u_id, uint32_t imb_ib_id, uint32_t imb_ib_i_id, uint32_t imb_ib_u_id, uint32_t imb_created, uint32_t imb_updated) : imb_i_id(imb_i_id), imb_u_id(imb_u_id), imb_ib_id(imb_ib_id), imb_ib_i_id(imb_ib_i_id), imb_ib_u_id(imb_ib_u_id), imb_created(imb_created), imb_updated(imb_updated) {}

    auto get_imb_i_id() -> uint32_t  { return imb_i_id; }
    auto get_imb_u_id() -> uint32_t  { return imb_u_id; }
    auto get_imb_ib_id() -> uint32_t  { return imb_ib_id; }
    auto get_imb_ib_i_id() -> uint32_t  { return imb_ib_i_id; }
    auto get_imb_ib_u_id() -> uint32_t  { return imb_ib_u_id; }
    auto get_imb_created() -> uint32_t  { return imb_created; }
    auto get_imb_updated() -> uint32_t  { return imb_updated; }

    void set_imb_i_id(uint32_t imb_i_id) { this->imb_i_id = imb_i_id; }
    void set_imb_u_id(uint32_t imb_u_id) { this->imb_u_id = imb_u_id; }
    void set_imb_ib_id(uint32_t imb_ib_id) { this->imb_ib_id = imb_ib_id; }
    void set_imb_ib_i_id(uint32_t imb_ib_i_id) { this->imb_ib_i_id = imb_ib_i_id; }
    void set_imb_ib_u_id(uint32_t imb_ib_u_id) { this->imb_ib_u_id = imb_ib_u_id; }
    void set_imb_created(uint32_t imb_created) { this->imb_created = imb_created; }
    void set_imb_updated(uint32_t imb_updated) { this->imb_updated = imb_updated; }
};

class ItemPurchaseRow {
 private:
    uint32_t ip_id;
    uint32_t ip_ib_id;
    uint32_t ip_i_id;
    uint32_t ip_u_id;
    uint32_t ip_date;

 public:
    ItemPurchaseRow() {}
    ItemPurchaseRow(uint32_t ip_id, uint32_t ip_ib_id, uint32_t ip_i_id, uint32_t ip_u_id, uint32_t ip_date) : ip_id(ip_id), ip_ib_id(ip_ib_id), ip_i_id(ip_i_id), ip_u_id(ip_u_id), ip_date(ip_date) {}

    auto get_ip_id() -> uint32_t  { return ip_id; }
    auto get_ip_ib_id() -> uint32_t  { return ip_ib_id; }
    auto get_ip_i_id() -> uint32_t  { return ip_i_id; }
    auto get_ip_u_id() -> uint32_t  { return ip_u_id; }
    auto get_ip_date() -> uint32_t  { return ip_date; }

    void set_ip_id(uint32_t ip_id) { this->ip_id = ip_id; }
    void set_ip_ib_id(uint32_t ip_ib_id) { this->ip_ib_id = ip_ib_id; }
    void set_ip_i_id(uint32_t ip_i_id) { this->ip_i_id = ip_i_id; }
    void set_ip_u_id(uint32_t ip_u_id) { this->ip_u_id = ip_u_id; }
    void set_ip_date(uint32_t ip_date) { this->ip_date = ip_date; }
};

class UserItemRow {
 private:
    uint32_t ui_u_id;
    uint32_t ui_i_id;
    uint32_t ui_seller_id;
    uint32_t ui_created;

 public:
    UserItemRow() {}
    UserItemRow(uint32_t ui_u_id, uint32_t ui_i_id, uint32_t ui_seller_id, uint32_t ui_created) : ui_u_id(ui_u_id), ui_i_id(ui_i_id), ui_seller_id(ui_seller_id), ui_created(ui_created) {}

    auto get_ui_u_id() -> uint32_t  { return ui_u_id; }
    auto get_ui_i_id() -> uint32_t  { return ui_i_id; }
    auto get_ui_seller_id() -> uint32_t  { return ui_seller_id; }
    auto get_ui_created() -> uint32_t  { return ui_created; }

    void set_ui_u_id(uint32_t ui_u_id) { this->ui_u_id = ui_u_id; }
    void set_ui_i_id(uint32_t ui_i_id) { this->ui_i_id = ui_i_id; }
    void set_ui_seller_id(uint32_t ui_seller_id) { this->ui_seller_id = ui_seller_id; }
    void set_ui_created(uint32_t ui_created) { this->ui_created = ui_created; }
};

}

#endif /* AUCTIONMARK_SCHEMA_H */
