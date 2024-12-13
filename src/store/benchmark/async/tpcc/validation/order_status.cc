/***********************************************************************
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
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
#include "store/benchmark/async/tpcc/validation/order_status.h"
#include "store/benchmark/async/tpcc/tpcc_utils.h"
  
namespace tpcc {

ValidationOrderStatus::ValidationOrderStatus(uint32_t timeout, uint32_t w_id, uint32_t d_id, uint32_t c_w_id, 
    uint32_t c_d_id, uint32_t c_id, uint32_t o_id, bool c_by_last_name, std::string c_last) :
    ValidationTPCCTransaction(timeout) {
  this->w_id = w_id;
  this->d_id = d_id;
  this->c_w_id = c_w_id;
  this->c_d_id = c_d_id;
  this->c_id = c_id;
  this->o_id = o_id; 
  this->c_by_last_name = c_by_last_name;
  this->c_last = c_last;
}

ValidationOrderStatus::ValidationOrderStatus(uint32_t timeout, validation::proto::OrderStatus valOrderStatusMsg) : 
    ValidationTPCCTransaction(timeout) {
  w_id = valOrderStatusMsg.w_id();
  d_id = valOrderStatusMsg.d_id();
  c_w_id = valOrderStatusMsg.c_w_id();
  c_d_id = valOrderStatusMsg.c_d_id();
  c_id = valOrderStatusMsg.c_id();
  o_id = valOrderStatusMsg.o_id();
  c_by_last_name = valOrderStatusMsg.c_by_last_name();
  c_last = valOrderStatusMsg.c_last();
}

ValidationOrderStatus::~ValidationOrderStatus() {
}

transaction_status_t ValidationOrderStatus::Validate(::SyncClient &client) {
  std::string str;
  std::vector<std::string> strs;

  Debug("ORDER_STATUS");
  Debug("Warehouse: %u", c_w_id);
  Debug("District: %u", c_d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());
    std::string cbn_key = CustomerByNameRowKey(c_w_id, c_d_id, c_last);
    client.Get(cbn_key, str, timeout);
    CustomerByNameRow cbn_row;
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

  std::string c_key = CustomerRowKey(c_w_id, c_d_id, c_id);
  client.Get(c_key, timeout);
  std::string obc_key = OrderByCustomerRowKey(c_w_id, c_d_id, c_id);
  client.Get(obc_key, timeout);

  client.Wait(strs);

  CustomerRow c_row;
  UW_ASSERT(c_row.ParseFromString(strs[0]));
  Debug("  First: %s", c_row.first().c_str());
  Debug("  Last: %s", c_row.last().c_str());

  OrderByCustomerRow obc_row;
  UW_ASSERT(obc_row.ParseFromString(strs[1]));

  strs.clear();

  o_id = obc_row.o_id();
  Debug("Order: %u", o_id);
  std::string o_key = OrderRowKey(c_w_id, c_d_id, o_id);
  client.Get(o_key, str, timeout);
  OrderRow o_row;
  if(str.empty()) Panic("empty string for Order Row");
  UW_ASSERT(o_row.ParseFromString(str));
  Debug("  Order Lines: %u", o_row.ol_cnt());
  Debug("  Entry Date: %u", o_row.entry_d());
  Debug("  Carrier ID: %u", o_row.carrier_id());

  for (size_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
    client.Get(OrderLineRowKey(c_w_id, c_d_id, o_id, ol_number), timeout);
  }

  client.Wait(strs);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc
