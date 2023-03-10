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
#include "store/benchmark/async/sql/tpcc/order_status.h"

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLOrderStatus::SQLOrderStatus(uint32_t timeout, uint32_t w_id,
    uint32_t c_c_last, uint32_t c_c_id, std::mt19937 &gen) :
    TPCCSQLTransaction(timeout), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  int y = std::uniform_int_distribution<int>(1, 100)(gen);
  c_w_id = w_id;
  c_d_id = d_id;
  if (y <= 60) {
    int last = tpcc::NURand(255, 0, 999, static_cast<int>(c_c_last), gen);
    c_last = tpcc::GenerateCustomerLastName(last);
    c_by_last_name = true;
  } else {
    c_id = tpcc::NURand(1023, 1, 3000, static_cast<int>(c_c_id), gen);
    c_by_last_name = false;
  }
}

SQLOrderStatus::~SQLOrderStatus() {
}

transaction_status_t SQLOrderStatus::Execute(SyncClient &client) {
  std::string str;
  std::vector<std::string> strs;

  Debug("ORDER_STATUS");
  Debug("Warehouse: %u", c_w_id);
  Debug("District: %u", c_d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());
    std::string cbn_key = tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last);
    client.Get(cbn_key, str, timeout);
    tpcc::CustomerByNameRow cbn_row;
    UW_ASSERT(cbn_row.ParseFromString(str));
    int idx = (cbn_row.ids_size() + 1) / 2;
    if (idx == cbn_row.ids_size()) {
      idx = cbn_row.ids_size() - 1;
    }
    c_id = cbn_row.ids(idx);
    Debug("  ID: %u", c_id);
  } else {
    Debug("Customer: %u", c_id);
  }

  std::string c_key = tpcc::CustomerRowKey(c_w_id, c_d_id, c_id);
  client.Get(c_key, timeout);
  std::string obc_key = tpcc::OrderByCustomerRowKey(c_w_id, c_d_id, c_id);
  client.Get(obc_key, timeout);

  client.Wait(strs);

  tpcc::CustomerRow c_row;
  UW_ASSERT(c_row.ParseFromString(strs[0]));
  Debug("  First: %s", c_row.first().c_str());
  Debug("  Last: %s", c_row.last().c_str());

  tpcc::OrderByCustomerRow obc_row;
  UW_ASSERT(obc_row.ParseFromString(strs[1]));

  strs.clear();

  o_id = obc_row.o_id();
  Debug("Order: %u", o_id);
  std::string o_key = tpcc::OrderRowKey(c_w_id, c_d_id, o_id);
  client.Get(o_key, str, timeout);
  tpcc::OrderRow o_row;
  if(str.empty()) Panic("empty string for Order Row");
  UW_ASSERT(o_row.ParseFromString(str));
  Debug("  Order Lines: %u", o_row.ol_cnt());
  Debug("  Entry Date: %u", o_row.entry_d());
  Debug("  Carrier ID: %u", o_row.carrier_id());

  for (size_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
    client.Get(tpcc::OrderLineRowKey(c_w_id, c_d_id, o_id, ol_number), timeout);
  }

  client.Wait(strs);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
