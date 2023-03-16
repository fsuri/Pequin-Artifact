/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#include "store/benchmark/async/sql/tpcc/new_order.h"

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLNewOrder::SQLNewOrder(uint32_t timeout, uint32_t w_id, uint32_t C,
    uint32_t num_warehouses, std::mt19937 &gen) :
    TPCCSQLTransaction(timeout), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  c_id = tpcc::NURand(static_cast<uint32_t>(1023), static_cast<uint32_t>(1), static_cast<uint32_t>(3000), C, gen);
  ol_cnt = std::uniform_int_distribution<uint8_t>(5, 15)(gen);
  rbk = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
  all_local = true;
  for (uint8_t i = 0; i < ol_cnt; ++i) {
    if (rbk == 1 && i == ol_cnt - 1) {
      o_ol_i_ids.push_back(0);
    } else {
      o_ol_i_ids.push_back(tpcc::NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1), static_cast<uint32_t>(100000), C, gen));
    }
    uint8_t x = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
    if (x == 1 && num_warehouses > 1) {
      uint32_t remote_w_id = std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
      if (remote_w_id == w_id) {
        remote_w_id = num_warehouses; // simple swap to ensure uniform distribution
      }
      o_ol_supply_w_ids.push_back(remote_w_id);
      all_local = false;
    } else {
      o_ol_supply_w_ids.push_back(w_id);
    }
    o_ol_quantities.push_back(std::uniform_int_distribution<uint8_t>(1, 10)(gen));
  }
  o_entry_d = std::time(0);
}

SQLNewOrder::~SQLNewOrder() {
}

transaction_status_t SQLNewOrder::Execute(SyncClient &client) {
  std::string str;

  Debug("NEW_ORDER");
  Debug("Warehouse: %u", w_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  client.Get(tpcc::WarehouseRowKey(w_id), timeout);
  Debug("District: %u", d_id);
  std::string d_key = tpcc::DistrictRowKey(w_id, d_id);
  client.Get(d_key, timeout);
  Debug("Customer: %u", c_id);
  client.Get(tpcc::CustomerRowKey(w_id, d_id, c_id), timeout);

  std::vector<std::string> strs;
  client.Wait(strs);

  tpcc::WarehouseRow w_row;
  UW_ASSERT(w_row.ParseFromString(strs[0]));
  Debug("  Tax Rate: %u", w_row.tax());

  tpcc::DistrictRow d_row;
  UW_ASSERT(d_row.ParseFromString(strs[1]));
  Debug("  Tax Rate: %u", d_row.tax());
  uint32_t o_id = d_row.next_o_id();
  Debug("  Order Number: %u", o_id);

  d_row.set_next_o_id(d_row.next_o_id() + 1);
  d_row.SerializeToString(&str);
  client.Put(d_key, str, timeout);

  tpcc::CustomerRow c_row;
  UW_ASSERT(c_row.ParseFromString(strs[2]));
  Debug("  Discount: %i", c_row.discount());
  Debug("  Last Name: %s", c_row.last().c_str());
  Debug("  Credit: %s", c_row.credit().c_str());

  strs.clear();

  tpcc::NewOrderRow no_row;
  no_row.set_o_id(o_id);
  no_row.set_d_id(d_id);
  no_row.set_w_id(w_id);
  no_row.SerializeToString(&str);
  client.Put(tpcc::NewOrderRowKey(w_id, d_id, o_id), str, timeout);

  tpcc::OrderRow o_row;
  o_row.set_id(o_id);
  o_row.set_d_id(d_id);
  o_row.set_w_id(w_id);
  o_row.set_c_id(c_id);
  o_row.set_entry_d(o_entry_d);
  o_row.set_carrier_id(0);
  o_row.set_ol_cnt(ol_cnt);
  o_row.set_all_local(all_local);
  o_row.SerializeToString(&str);
  client.Put(tpcc::OrderRowKey(w_id, d_id, o_id), str, timeout);

  tpcc::OrderByCustomerRow obc_row;
  obc_row.set_w_id(w_id);
  obc_row.set_d_id(d_id);
  obc_row.set_c_id(c_id);
  obc_row.set_o_id(o_id);
  obc_row.SerializeToString(&str);
  client.Put(tpcc::OrderByCustomerRowKey(w_id, d_id, c_id), str, timeout);

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Item: %u", o_ol_i_ids[ol_number]);
    std::string i_key = tpcc::ItemRowKey(o_ol_i_ids[ol_number]);
    client.Get(i_key, timeout);
  }

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Supply Warehouse: %u", o_ol_supply_w_ids[ol_number]);
    client.Get(tpcc::StockRowKey(o_ol_supply_w_ids[ol_number], o_ol_i_ids[ol_number]),
        timeout);
  }

  client.Wait(strs);

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    if (strs[ol_number].empty()) {
      client.Abort(timeout);
      return ABORTED_USER;
    } else {
      tpcc::ItemRow i_row;
      UW_ASSERT(i_row.ParseFromString(strs[ol_number]));
      Debug("    Item Name: %s", i_row.name().c_str());

      tpcc::StockRow s_row;
      std::string s_key = tpcc::StockRowKey(o_ol_supply_w_ids[ol_number],
          o_ol_i_ids[ol_number]);
      UW_ASSERT(s_row.ParseFromString(strs[ol_number + ol_cnt]));

      if (s_row.quantity() - o_ol_quantities[ol_number] >= 10) {
        s_row.set_quantity(s_row.quantity() - o_ol_quantities[ol_number]);
      } else {
        s_row.set_quantity(s_row.quantity() - o_ol_quantities[ol_number] + 91);
      }
      Debug("    Quantity: %u", o_ol_quantities[ol_number]);
      s_row.set_ytd(s_row.ytd() + o_ol_quantities[ol_number]);
      s_row.set_order_cnt(s_row.order_cnt() + 1);
      Debug("    Remaining Quantity: %u", s_row.quantity());
      Debug("    YTD: %u", s_row.ytd());
      Debug("    Order Count: %u", s_row.order_cnt());
      if (w_id != o_ol_supply_w_ids[ol_number]) {
        s_row.set_remote_cnt(s_row.remote_cnt() + 1);
      }
      s_row.SerializeToString(&str);
      client.Put(s_key, str, timeout);

      tpcc::OrderLineRow ol_row;
      ol_row.set_o_id(o_id);
      ol_row.set_d_id(d_id);
      ol_row.set_w_id(w_id);
      ol_row.set_number(ol_number);
      ol_row.set_i_id(o_ol_i_ids[ol_number]);
      ol_row.set_supply_w_id(o_ol_supply_w_ids[ol_number]);
      ol_row.set_delivery_d(0);
      ol_row.set_quantity(o_ol_quantities[ol_number]);
      ol_row.set_amount(o_ol_quantities[ol_number] * i_row.price());
      switch (d_id) {
        case 1:
          ol_row.set_dist_info(s_row.dist_01());
          break;
        case 2:
          ol_row.set_dist_info(s_row.dist_02());
          break;
        case 3:
          ol_row.set_dist_info(s_row.dist_03());
          break;
        case 4:
          ol_row.set_dist_info(s_row.dist_04());
          break;
        case 5:
          ol_row.set_dist_info(s_row.dist_05());
          break;
        case 6:
          ol_row.set_dist_info(s_row.dist_06());
          break;
        case 7:
          ol_row.set_dist_info(s_row.dist_07());
          break;
        case 8:
          ol_row.set_dist_info(s_row.dist_08());
          break;
        case 9:
          ol_row.set_dist_info(s_row.dist_09());
          break;
        case 10:
          ol_row.set_dist_info(s_row.dist_10());
          break;
        default:
          NOT_REACHABLE();
      }
      ol_row.SerializeToString(&str);
      client.Put(tpcc::OrderLineRowKey(w_id, d_id, o_id, ol_number), str, timeout);
    }
  }

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
