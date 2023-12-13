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

class WarehouseRow {
 private:
    uint32_t id;
    std::string name;
    std::string street_1;
    std::string street_2;
    std::string city;
    std::string state;
    std::string zip;
    int32_t tax;
    int32_t ytd;

 public:
    inline void set_id(uint32_t id) { this->id = id; }
    inline void set_name(std::string name) { this->name = name; }
    inline void set_street_1(std::string street_1) { this->street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->street_2 = street_2; }
    inline void set_city(std::string city) { this->city = city; }
    inline void set_state(std::string state) { this->state = state; }
    inline void set_zip(std::string zip) { this->zip = zip; }
    inline void set_tax(int32_t tax) { this->tax = tax; }
    inline void set_ytd(int32_t ytd) { this->ytd = ytd; }

    inline auto get_id() { return id; }
    inline auto get_name() -> std::string& { return name; }
    inline auto get_street_1() -> std::string& { return street_1; }
    inline auto get_street_2() -> std::string& { return street_2; }
    inline auto get_city() -> std::string& { return city; }
    inline auto get_state() -> std::string& { return state; }
    inline auto get_zip() -> std::string& { return zip; }
    inline auto get_tax() { return tax; }
    inline auto get_ytd() { return ytd; }
};

struct DistrictRow {
 private:
    uint32_t id;
    uint32_t w_id;
    std::string name;
    std::string street_1;
    std::string street_2;
    std::string city;
    std::string state;
    std::string zip;
    int32_t tax;
    int32_t ytd;
    uint32_t next_o_id;

 public:
    inline void set_id(uint32_t id) { this->id = id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }
    inline void set_name(std::string name) { this->name = name; }
    inline void set_street_1(std::string street_1) { this->street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->street_2 = street_2; }
    inline void set_city(std::string city) { this->city = city; }
    inline void set_state(std::string state) { this->state = state; }
    inline void set_zip(std::string zip) { this->zip = zip; }
    inline void set_tax(int32_t tax) { this->tax = tax; }
    inline void set_ytd(int32_t ytd) { this->ytd = ytd; }
    inline void set_next_o_id(uint32_t next_o_id) { this->next_o_id = next_o_id; }

    inline auto get_id() { return id; }
    inline auto get_w_id() { return w_id; }
    inline auto get_name() -> std::string& { return name; }
    inline auto get_street_1() -> std::string& { return street_1; }
    inline auto get_street_2() -> std::string& { return street_2; }
    inline auto get_city() -> std::string& { return city; }
    inline auto get_state() -> std::string& { return state; }
    inline auto get_zip() -> std::string& { return zip; }
    inline auto get_tax() { return tax; }
    inline auto get_ytd() { return ytd; }
    inline auto get_next_o_id() { return next_o_id; }
};

class OrderRow {
 private:
    uint32_t id;
    uint32_t d_id;
    uint32_t w_id;
    uint32_t c_id;
    uint32_t entry_d;
    uint32_t carrier_id;
    uint32_t ol_cnt;
    bool all_local;

 public:
    inline void set_id(uint32_t id) { this->id = id; }
    inline void set_d_id(uint32_t d_id) { this->d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }
    inline void set_c_id(uint32_t c_id) { this->c_id = c_id; }
    inline void set_entry_d(uint32_t entry_d) { this->entry_d = entry_d; }
    inline void set_carrier_id(uint32_t carrier_id) { this->carrier_id = carrier_id; }
    inline void set_ol_cnt(uint32_t ol_cnt) { this->ol_cnt = ol_cnt; }
    inline void set_all_local(bool all_local) { this->all_local = all_local; }

    inline auto get_id() { return id; }
    inline auto get_d_id() { return d_id; }
    inline auto get_w_id() { return w_id; }
    inline auto get_c_id() { return c_id; }
    inline auto get_entry_d() { return entry_d; }
    inline auto get_carrier_id() { return carrier_id; }
    inline auto get_ol_cnt() { return ol_cnt; }
    inline auto get_all_local() { return all_local; }
};

class OrderLineRow {
 private:
    uint32_t o_id;
    uint32_t d_id;
    uint32_t w_id;
    uint32_t number;
    uint32_t i_id;
    uint32_t supply_w_id;
    uint32_t delivery_d;
    uint32_t quantity;
    int32_t amount;
    std::string dist_info;

 public:
    inline void set_o_id(uint32_t o_id) { this->o_id = o_id; }
    inline void set_d_id(uint32_t d_id) { this->d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }
    inline void set_number(uint32_t number) { this->number = number; }
    inline void set_i_id(uint32_t i_id) { this->i_id = i_id; }
    inline void set_supply_w_id(uint32_t supply_w_id) { this->supply_w_id = supply_w_id; }
    inline void set_delivery_d(uint32_t delivery_d) { this->delivery_d = delivery_d; }
    inline void set_quantity(uint32_t quantity) { this->quantity = quantity; }
    inline void set_amount(int32_t amount) { this->amount = amount; }
    inline void set_dist_info(std::string dist_info) { this->dist_info = dist_info; }

    inline auto get_o_id() { return o_id; }
    inline auto get_d_id() { return d_id; }
    inline auto get_w_id() { return w_id; }
    inline auto get_number() { return number; }
    inline auto get_i_id() { return i_id; }
    inline auto get_supply_w_id() { return supply_w_id; }
    inline auto get_delivery_d() { return delivery_d; }
    inline auto get_quantity() { return quantity; }
    inline auto get_amount() { return amount; }
    inline auto get_dist_info() -> std::string& { return dist_info; }
};

class CustomerRow {
 private:
    uint32_t id;
    uint32_t d_id;
    uint32_t w_id;
    std::string first;
    std::string middle;
    std::string last;
    std::string street_1;
    std::string street_2;
    std::string city;
    std::string state;
    std::string zip;
    std::string phone;
    uint32_t since;
    std::string credit;
    uint32_t credit_lim;
    int32_t discount;
    int32_t balance;
    int32_t ytd_payment;
    uint32_t payment_cnt;
    uint32_t delivery_cnt;
    std::string data;

 public:
    inline void set_id(uint32_t id) { this->id = id; }
    inline void set_d_id(uint32_t d_id) { this->d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }
    inline void set_first(std::string first) { this->first = first; }
    inline void set_middle(std::string middle) { this->middle = middle; }
    inline void set_last(std::string last) { this->last = last; }
    inline void set_street_1(std::string street_1) { this->street_1 = street_1; }
    inline void set_street_2(std::string street_2) { this->street_2 = street_2; }
    inline void set_city(std::string city) { this->city = city; }
    inline void set_state(std::string state) { this->state = state; }
    inline void set_zip(std::string zip) { this->zip = zip; }
    inline void set_phone(std::string phone) { this->phone = phone; }
    inline void set_since(uint32_t since) { this->since = since; }
    inline void set_credit(std::string credit) { this->credit = credit; }
    inline void set_credit_lim(uint32_t credit_lim) { this->credit_lim = credit_lim; }
    inline void set_discount(int32_t discount) { this->discount = discount; }
    inline void set_balance(int32_t balance) { this->balance = balance; }
    inline void set_ytd_payment(int32_t ytd_payment) { this->ytd_payment = ytd_payment; }
    inline void set_payment_cnt(uint32_t payment_cnt) { this->payment_cnt = payment_cnt; }
    inline void set_delivery_cnt(uint32_t delivery_cnt) { this->delivery_cnt = delivery_cnt; }
    inline void set_data(std::string data) { this->data = data; }

    inline auto get_id() { return id; }
    inline auto get_d_id() { return d_id; }
    inline auto get_w_id() { return w_id; }
    inline auto get_first() -> std::string& { return first; }
    inline auto get_middle() -> std:: string& { return middle; }
    inline auto get_last() -> std::string& { return last; }
    inline auto get_street_1() -> std::string& { return street_1; }
    inline auto get_street_2() -> std::string& { return street_2; }
    inline auto get_city() -> std::string& { return city; }
    inline auto get_state() -> std::string& { return state; }
    inline auto get_zip() -> std::string& { return zip; }
    inline auto get_phone() -> std::string& { return phone; }
    inline auto get_since() { return since; }
    inline auto get_credit() -> std::string& { return credit; }
    inline auto get_credit_lim() { return credit_lim; }
    inline auto get_discount() { return discount; }
    inline auto get_balance() { return balance; }
    inline auto get_ytd_payment() { return ytd_payment; }
    inline auto get_payment_cnt() { return payment_cnt; }
    inline auto get_delivery_cnt() { return delivery_cnt; }
    inline auto get_data() -> std::string& { return data; }
};

class ItemRow {
 private:
    uint32_t id;
    uint32_t im_id;
    std::string name;
    uint32_t price;
    std::string data;

 public:
    inline void set_id(uint32_t id) { this->id = id; }
    inline void set_im_id(uint32_t im_id) { this->im_id = im_id; }
    inline void set_name(std::string name) { this->name = name; }
    inline void set_price(uint32_t price) { this->price = price; }
    inline void set_data(std::string data) { this->data = data; }

    inline auto get_id() { return id; }
    inline auto get_im_id() { return im_id; }
    inline auto get_name() -> std::string& { return name; }
    inline auto get_price() { return price; }
    inline auto get_data() -> std::string& { return data; }
};

class StockRow {
 private:
    uint32_t i_id;
    uint32_t w_id;
    uint32_t quantity;
    std::string dist_01;
    std::string dist_02;
    std::string dist_03;
    std::string dist_04;
    std::string dist_05;
    std::string dist_06;
    std::string dist_07;
    std::string dist_08;
    std::string dist_09;
    std::string dist_10;
    int32_t ytd;
    int32_t order_cnt;
    int32_t remote_cnt;
    std::string data;

 public:
    inline void set_i_id(uint32_t i_id) { this->i_id = i_id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }
    inline void set_quantity(uint32_t quantity) { this->quantity = quantity; }
    inline void set_dist_01(std::string dist_01) { this->dist_01 = dist_01; }
    inline void set_dist_02(std::string dist_02) { this->dist_02 = dist_02; }
    inline void set_dist_03(std::string dist_03) { this->dist_03 = dist_03; }
    inline void set_dist_04(std::string dist_04) { this->dist_04 = dist_04; }
    inline void set_dist_05(std::string dist_05) { this->dist_05 = dist_05; }
    inline void set_dist_06(std::string dist_06) { this->dist_06 = dist_06; }
    inline void set_dist_07(std::string dist_07) { this->dist_07 = dist_07; }
    inline void set_dist_08(std::string dist_08) { this->dist_08 = dist_08; }
    inline void set_dist_09(std::string dist_09) { this->dist_09 = dist_09; }
    inline void set_dist_10(std::string dist_10) { this->dist_10 = dist_10; }
    inline void set_ytd(int32_t ytd) { this->ytd = ytd; }
    inline void set_order_cnt(int32_t order_cnt) { this->order_cnt = order_cnt; }
    inline void set_remote_cnt(int32_t remote_cnt) { this->remote_cnt = remote_cnt; }
    inline void set_data(std::string data) { this->data = data; }

    inline auto get_i_id() { return i_id; }
    inline auto get_w_id() { return w_id; }
    inline auto get_quantity() { return quantity; }
    inline auto get_dist_01() -> std::string& { return dist_01; }
    inline auto get_dist_02() -> std::string& { return dist_02; }
    inline auto get_dist_03() -> std::string& { return dist_03; }
    inline auto get_dist_04() -> std::string& { return dist_04; }
    inline auto get_dist_05() -> std::string& { return dist_05; }
    inline auto get_dist_06() -> std::string& { return dist_06; }
    inline auto get_dist_07() -> std::string& { return dist_07; }
    inline auto get_dist_08() -> std::string& { return dist_08; }
    inline auto get_dist_09() -> std::string& { return dist_09; }
    inline auto get_dist_10() -> std::string& { return dist_10; }
    inline auto get_ytd() { return ytd; }
    inline auto get_order_cnt() { return order_cnt; }
    inline auto get_remote_cnt() { return remote_cnt; }
    inline auto get_data() -> std::string& { return data; }
};

struct NewOrderRow {
 private:
    uint32_t o_id;
    uint32_t d_id;
    uint32_t w_id;

 public:
    inline void set_o_id(uint32_t o_id) { this->o_id = o_id; }
    inline void set_d_id(uint32_t d_id) { this->d_id = d_id; }
    inline void set_w_id(uint32_t w_id) { this->w_id = w_id; }

    inline auto get_o_id() { return o_id; }
    inline auto get_d_id() { return d_id; }
    inline auto get_w_id() { return w_id; }
};

}

#endif /* TPCC_SQL_SCHEMA_H */
