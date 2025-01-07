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
#ifndef TPCC_SQL_SCHEMA_H
#define TPCC_SQL_SCHEMA_H

#include <stdint.h>

namespace tpcc_sql {

//Table Names
const std::string WAREHOUSE_TABLE = "warehouse";
const std::string DISTRICT_TABLE = "district";
const std::string CUSTOMER_TABLE = "customer";
const std::string ITEM_TABLE = "item";
const std::string STOCK_TABLE = "stock";
const std::string ORDER_TABLE = "oorder";//"\"Order\"";
const std::string HISTORY_TABLE = "history";
const std::string ORDER_LINE_TABLE = "order_line";
const std::string NEW_ORDER_TABLE = "new_order";

const std::string EARLIEST_NEW_ORDER_TABLE = "EarliestNewOrder";

enum TPCC_Table {
    WAREHOUSE, 
    DISTRICT, 
    ITEM,
    CUSTOMER,
    STOCK, 
    HISTORY,
    NEW_ORDER,
    ORDER,
    ORDER_LINE,
    EARLIEST_NEW_ORDER
  };

//Row Types

class WarehouseRow {
 private:
    uint32_t w_id;
    std::string w_name;
    std::string w_street_1;
    std::string w_street_2;
    std::string w_city;
    std::string w_state;
    std::string w_zip;
    int32_t w_tax;
    int32_t w_ytd;

 public:
    inline void set_id(uint32_t id) { this->w_id = id; }
    inline void set_name(std::string name) { this->w_name = name; }
    inline void set_street_1(std::string street_1) { this->w_street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->w_street_2 = street_2; }
    inline void set_city(std::string city) { this->w_city = city; }
    inline void set_state(std::string state) { this->w_state = state; }
    inline void set_zip(std::string zip) { this->w_zip = zip; }
    inline void set_tax(int32_t tax) { this->w_tax = tax; }
    inline void set_ytd(int32_t ytd) { this->w_ytd = ytd; }

    inline auto get_id() { return w_id; }
    inline auto get_name() -> std::string& { return w_name; }
    inline auto get_street_1() -> std::string& { return w_street_1; }
    inline auto get_street_2() -> std::string& { return w_street_2; }
    inline auto get_city() -> std::string& { return w_city; }
    inline auto get_state() -> std::string& { return w_state; }
    inline auto get_zip() -> std::string& { return w_zip; }
    inline auto get_tax() { return w_tax; }
    inline auto get_ytd() { return w_ytd; }
};

struct DistrictRow {
 private:
    uint32_t d_id;
    uint32_t d_w_id;
    std::string d_name;
    std::string d_street_1;
    std::string d_street_2;
    std::string d_city;
    std::string d_state;
    std::string d_zip;
    int32_t d_tax;
    int32_t d_ytd;
    uint32_t d_next_o_id;

 public:
    inline void set_id(uint32_t id) { this->d_id = id; }
    inline void set_w_id(uint32_t w_id) { this->d_w_id = w_id; }
    inline void set_name(std::string name) { this->d_name = name; }
    inline void set_street_1(std::string street_1) { this->d_street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->d_street_2 = street_2; }
    inline void set_city(std::string city) { this->d_city = city; }
    inline void set_state(std::string state) { this->d_state = state; }
    inline void set_zip(std::string zip) { this->d_zip = zip; }
    inline void set_tax(int32_t tax) { this->d_tax = tax; }
    inline void set_ytd(int32_t ytd) { this->d_ytd = ytd; }
    inline void set_next_o_id(uint32_t next_o_id) { this->d_next_o_id = next_o_id; }

    inline auto get_id() { return d_id; }
    inline auto get_w_id() { return d_w_id; }
    inline auto get_name() -> std::string& { return d_name; }
    inline auto get_street_1() -> std::string& { return d_street_1; }
    inline auto get_street_2() -> std::string& { return d_street_2; }
    inline auto get_city() -> std::string& { return d_city; }
    inline auto get_state() -> std::string& { return d_state; }
    inline auto get_zip() -> std::string& { return d_zip; }
    inline auto get_tax() { return d_tax; }
    inline auto get_ytd() { return d_ytd; }
    inline auto get_next_o_id() { return d_next_o_id; }
};

class OrderRow {
 private:
    uint32_t o_id;
    uint32_t o_d_id;
    uint32_t o_w_id;
    uint32_t o_c_id;
    uint32_t o_entry_d;
    uint32_t o_carrier_id;
    uint32_t o_ol_cnt;
    bool o_all_local;

 public:
    inline void set_id(uint32_t id) { this->o_id = id; }
    inline void set_d_id(uint32_t d_id) { this->o_d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->o_w_id = w_id; }
    inline void set_c_id(uint32_t c_id) { this->o_c_id = c_id; }
    inline void set_entry_d(uint32_t entry_d) { this->o_entry_d = entry_d; }
    inline void set_carrier_id(uint32_t carrier_id) { this->o_carrier_id = carrier_id; }
    inline void set_ol_cnt(uint32_t ol_cnt) { this->o_ol_cnt = ol_cnt; }
    inline void set_all_local(bool all_local) { this->o_all_local = all_local; }

    inline auto get_id() { return o_id; }
    inline auto get_d_id() { return o_d_id; }
    inline auto get_w_id() { return o_w_id; }
    inline auto get_c_id() { return o_c_id; }
    inline auto get_entry_d() { return o_entry_d; }
    inline auto get_carrier_id() { return o_carrier_id; }
    inline auto get_ol_cnt() { return o_ol_cnt; }
    inline auto get_all_local() { return o_all_local; }
};

class OrderLineRow {
 private:
    uint32_t ol_o_id;
    uint32_t ol_d_id;
    uint32_t ol_w_id;
    uint32_t ol_number;
    uint32_t ol_i_id;
    uint32_t ol_supply_w_id;
    uint32_t ol_delivery_d;
    uint32_t ol_quantity;
    int32_t ol_amount;
    std::string ol_dist_info;

 public:
    inline void set_o_id(uint32_t o_id) { this->ol_o_id = o_id; }
    inline void set_d_id(uint32_t d_id) { this->ol_d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->ol_w_id = w_id; }
    inline void set_number(uint32_t number) { this->ol_number = number; }
    inline void set_i_id(uint32_t i_id) { this->ol_i_id = i_id; }
    inline void set_supply_w_id(uint32_t supply_w_id) { this->ol_supply_w_id = supply_w_id; }
    inline void set_delivery_d(uint32_t delivery_d) { this->ol_delivery_d = delivery_d; }
    inline void set_quantity(uint32_t quantity) { this->ol_quantity = quantity; }
    inline void set_amount(int32_t amount) { this->ol_amount = amount; }
    inline void set_dist_info(std::string dist_info) { this->ol_dist_info = dist_info; }

    inline auto get_o_id() { return ol_o_id; }
    inline auto get_d_id() { return ol_d_id; }
    inline auto get_w_id() { return ol_w_id; }
    inline auto get_number() { return ol_number; }
    inline auto get_i_id() { return ol_i_id; }
    inline auto get_supply_w_id() { return ol_supply_w_id; }
    inline auto get_delivery_d() { return ol_delivery_d; }
    inline auto get_quantity() { return ol_quantity; }
    inline auto get_amount() { return ol_amount; }
    inline auto get_dist_info() -> std::string& { return ol_dist_info; }
};

class CustomerRow {
 private:
    uint32_t c_id;
    uint32_t c_d_id;
    uint32_t c_w_id;
    std::string c_first;
    std::string c_middle;
    std::string c_last;
    std::string c_street_1;
    std::string c_street_2;
    std::string c_city;
    std::string c_state;
    std::string c_zip;
    std::string c_phone;
    uint32_t c_since;
    std::string c_credit;
    uint32_t c_credit_lim;
    int32_t c_discount;
    int32_t c_balance;
    int32_t c_ytd_payment;
    uint32_t c_payment_cnt;
    uint32_t c_delivery_cnt;
    std::string c_data;

 public:
    inline void set_id(uint32_t id) { this->c_id = id; }
    inline void set_d_id(uint32_t d_id) { this->c_d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->c_w_id = w_id; }
    inline void set_first(std::string first) { this->c_first = first; }
    inline void set_middle(std::string middle) { this->c_middle = middle; }
    inline void set_last(std::string last) { this->c_last = last; }
    inline void set_street_1(std::string street_1) { this->c_street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->c_street_2 = street_2; }
    inline void set_city(std::string city) { this->c_city = city; }
    inline void set_state(std::string state) { this->c_state = state; }
    inline void set_zip(std::string zip) { this->c_zip = zip; }
    inline void set_phone(std::string phone) { this->c_phone = phone; }
    inline void set_since(uint32_t since) { this->c_since = since; }
    inline void set_credit(std::string credit) { this->c_credit = credit; }
    inline void set_credit_lim(uint32_t credit_lim) { this->c_credit_lim = credit_lim; }
    inline void set_discount(int32_t discount) { this->c_discount = discount; }
    inline void set_balance(int32_t balance) { this->c_balance = balance; }
    inline void set_ytd_payment(int32_t ytd_payment) { this->c_ytd_payment = ytd_payment; }
    inline void set_payment_cnt(uint32_t payment_cnt) { this->c_payment_cnt = payment_cnt; }
    inline void set_delivery_cnt(uint32_t delivery_cnt) { this->c_delivery_cnt = delivery_cnt; }
    inline void set_data(std::string data) { this->c_data = data; }

    inline auto get_id() { return c_id; }
    inline auto get_d_id() { return c_d_id; }
    inline auto get_w_id() { return c_w_id; }
    inline auto get_first() -> std::string& { return c_first; }
    inline auto get_middle() -> std:: string& { return c_middle; }
    inline auto get_last() -> std::string& { return c_last; }
    inline auto get_street_1() -> std::string& { return c_street_1; }
    inline auto get_street_2() -> std::string& { return c_street_2; }
    inline auto get_city() -> std::string& { return c_city; }
    inline auto get_state() -> std::string& { return c_state; }
    inline auto get_zip() -> std::string& { return c_zip; }
    inline auto get_phone() -> std::string& { return c_phone; }
    inline auto get_since() { return c_since; }
    inline auto get_credit() -> std::string& { return c_credit; }
    inline auto get_credit_lim() { return c_credit_lim; }
    inline auto get_discount() { return c_discount; }
    inline auto get_balance() { return c_balance; }
    inline auto get_ytd_payment() { return c_ytd_payment; }
    inline auto get_payment_cnt() { return c_payment_cnt; }
    inline auto get_delivery_cnt() { return c_delivery_cnt; }
    inline auto get_data() -> std::string& { return c_data; }
};

class ItemRow {
 private:
    uint32_t i_id;
    uint32_t i_im_id;
    std::string i_name;
    uint32_t i_price;
    std::string i_data;

 public:
    inline void set_id(uint32_t id) { this->i_id = id; }
    inline void set_im_id(uint32_t im_id) { this->i_im_id = im_id; }
    inline void set_name(std::string name) { this->i_name = name; }
    inline void set_price(uint32_t price) { this->i_price = price; }
    inline void set_data(std::string data) { this->i_data = data; }

    inline auto get_id() { return i_id; }
    inline auto get_im_id() { return i_im_id; }
    inline auto get_name() -> std::string& { return i_name; }
    inline auto get_price() { return i_price; }
    inline auto get_data() -> std::string& { return i_data; }
};

class StockRow {
 private:
    uint32_t s_i_id;
    uint32_t s_w_id;
    uint32_t s_quantity;
    std::string s_dist_01;
    std::string s_dist_02;
    std::string s_dist_03;
    std::string s_dist_04;
    std::string s_dist_05;
    std::string s_dist_06;
    std::string s_dist_07;
    std::string s_dist_08;
    std::string s_dist_09;
    std::string s_dist_10;
    int32_t s_ytd;
    int32_t s_order_cnt;
    int32_t s_remote_cnt;
    std::string s_data;

 public:
    inline void set_i_id(uint32_t i_id) { this->s_i_id = i_id; }
    inline void set_w_id(uint32_t w_id) { this->s_w_id = w_id; }
    inline void set_quantity(uint32_t quantity) { this->s_quantity = quantity; }
    inline void set_dist_01(std::string dist_01) { this->s_dist_01 = dist_01; }
    inline void set_dist_02(std::string dist_02) { this->s_dist_02 = dist_02; }
    inline void set_dist_03(std::string dist_03) { this->s_dist_03 = dist_03; }
    inline void set_dist_04(std::string dist_04) { this->s_dist_04 = dist_04; }
    inline void set_dist_05(std::string dist_05) { this->s_dist_05 = dist_05; }
    inline void set_dist_06(std::string dist_06) { this->s_dist_06 = dist_06; }
    inline void set_dist_07(std::string dist_07) { this->s_dist_07 = dist_07; }
    inline void set_dist_08(std::string dist_08) { this->s_dist_08 = dist_08; }
    inline void set_dist_09(std::string dist_09) { this->s_dist_09 = dist_09; }
    inline void set_dist_10(std::string dist_10) { this->s_dist_10 = dist_10; }
    inline void set_ytd(int32_t ytd) { this->s_ytd = ytd; }
    inline void set_order_cnt(int32_t order_cnt) { this->s_order_cnt = order_cnt; }
    inline void set_remote_cnt(int32_t remote_cnt) { this->s_remote_cnt = remote_cnt; }
    inline void set_data(std::string data) { this->s_data = data; }

    inline auto get_i_id() { return s_i_id; }
    inline auto get_w_id() { return s_w_id; }
    inline auto get_quantity() { return s_quantity; }
    inline auto get_dist_01() -> std::string& { return s_dist_01; }
    inline auto get_dist_02() -> std::string& { return s_dist_02; }
    inline auto get_dist_03() -> std::string& { return s_dist_03; }
    inline auto get_dist_04() -> std::string& { return s_dist_04; }
    inline auto get_dist_05() -> std::string& { return s_dist_05; }
    inline auto get_dist_06() -> std::string& { return s_dist_06; }
    inline auto get_dist_07() -> std::string& { return s_dist_07; }
    inline auto get_dist_08() -> std::string& { return s_dist_08; }
    inline auto get_dist_09() -> std::string& { return s_dist_09; }
    inline auto get_dist_10() -> std::string& { return s_dist_10; }
    inline auto get_ytd() { return s_ytd; }
    inline auto get_order_cnt() { return s_order_cnt; }
    inline auto get_remote_cnt() { return s_remote_cnt; }
    inline auto get_data() -> std::string& { return s_data; }
};

struct NewOrderRow {
 private:
    uint32_t no_o_id;
    uint32_t no_d_id;
    uint32_t no_w_id;

 public:
    inline void set_o_id(uint32_t o_id) { this->no_o_id = o_id; }
    inline void set_d_id(uint32_t d_id) { this->no_d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->no_w_id = w_id; }

    inline auto get_o_id() { return no_o_id; }
    inline auto get_d_id() { return no_d_id; }
    inline auto get_w_id() { return no_w_id; }
};

}

#endif /* TPCC_SQL_SCHEMA_H */
