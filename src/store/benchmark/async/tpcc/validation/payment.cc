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
#include "store/benchmark/async/tpcc/validation/payment.h"
#include "store/benchmark/async/tpcc/tpcc_utils.h"

#include <sstream>
  
namespace tpcc {

ValidationPayment::ValidationPayment(uint32_t timeout, uint32_t w_id, uint32_t d_id, uint32_t d_w_id, uint32_t c_w_id,
    uint32_t c_d_id, uint32_t c_id, uint32_t h_amount, uint32_t h_date, bool c_by_last_name, std::string c_last) :
    ValidationTPCCTransaction(timeout) {
  this->w_id = w_id;
  this->d_id = d_id;
  this->d_w_id = d_w_id;
  this->c_w_id = c_w_id;
  this->c_d_id = c_d_id;
  this->c_id = c_id;
  this->h_amount = h_amount;
  this->h_date = h_date;
  this->c_by_last_name = c_by_last_name;
  this->c_last = c_last;
}
ValidationPayment::ValidationPayment(uint32_t timeout, const validation::proto::Payment &valPaymentMsg) : 
    ValidationTPCCTransaction(timeout) {
  w_id = valPaymentMsg.w_id();
  d_id = valPaymentMsg.d_id();
  d_w_id = valPaymentMsg.d_w_id();
  c_w_id = valPaymentMsg.c_w_id();
  c_d_id = valPaymentMsg.c_d_id();
  c_id = valPaymentMsg.c_id();
  h_amount = valPaymentMsg.h_amount();
  h_date = valPaymentMsg.h_date();
  c_by_last_name = valPaymentMsg.c_by_last_name();
  c_last = valPaymentMsg.c_last();
}

ValidationPayment::~ValidationPayment() {
}

transaction_status_t ValidationPayment::Validate(::SyncClient &client) {
  std::string str;
  std::vector<std::string> strs;

  Debug("PAYMENT");
  Debug("Amount: %u", h_amount);
  Debug("Warehouse: %u", w_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  std::string w_key = WarehouseRowKey(w_id);
  client.Get(w_key, timeout);
  Debug("District: %u", d_id);
  std::string d_key = DistrictRowKey(d_w_id, d_id);
  client.Get(d_key, timeout);

  std::string c_key;
  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());
    Debug("  Get(c_w_id=%u, c_d_id=%u, c_last=%s)", c_w_id, c_d_id,
      c_last.c_str());
    std::string cbn_key = CustomerByNameRowKey(c_w_id, c_d_id, c_last);
    client.Get(cbn_key, timeout);

    client.Wait(strs);

    CustomerByNameRow cbn_row;
    UW_ASSERT(cbn_row.ParseFromString(strs[2]));
    int idx = (cbn_row.ids_size() + 1) / 2;
    if (idx == cbn_row.ids_size()) {
      idx = cbn_row.ids_size() - 1;
    }
    c_id = cbn_row.ids(idx);
    Debug("  ID: %u", c_id);

    c_key = CustomerRowKey(c_w_id, c_d_id, c_id);
    client.Get(CustomerRowKey(c_w_id, c_d_id, c_id), strs[2], timeout);
  } else {
    Debug("Customer: %u", c_id);

    c_key = CustomerRowKey(c_w_id, c_d_id, c_id);
    client.Get(CustomerRowKey(c_w_id, c_d_id, c_id), timeout);
    client.Wait(strs);
  }

  WarehouseRow w_row;
  UW_ASSERT(w_row.ParseFromString(strs[0]));
  w_row.set_ytd(w_row.ytd() + h_amount);
  Debug("  YTD: %u", w_row.ytd());
  w_row.SerializeToString(&str);
  client.Put(w_key, str, timeout);

  DistrictRow d_row;
  UW_ASSERT(d_row.ParseFromString(strs[1]));
  d_row.set_ytd(d_row.ytd() + h_amount);
  Debug("  YTD: %u", d_row.ytd());
  d_row.SerializeToString(&str);
  client.Put(d_key, str, timeout);

  CustomerRow c_row;
  UW_ASSERT(c_row.ParseFromString(strs[2]));
  c_row.set_balance(c_row.balance() - h_amount);
  c_row.set_ytd_payment(c_row.ytd_payment() + h_amount);
  c_row.set_payment_cnt(c_row.payment_cnt() + 1);
  Debug("  Balance: %u", c_row.balance());
  Debug("  YTD: %u", c_row.ytd_payment());
  Debug("  Payment Count: %u", c_row.payment_cnt());
  if (c_row.credit() == "BC") {
    std::stringstream ss;
    ss << c_id << "," << c_d_id << "," << c_w_id << "," << d_id << ","
             << w_id << "," << h_amount;
    std::string new_data = ss.str() +  c_row.data();
    new_data = new_data.substr(std::min(new_data.size(), 500UL));
    c_row.set_data(new_data);
  }
  c_row.SerializeToString(&str);
  client.Put(c_key, str, timeout);

  HistoryRow h_row;
  h_row.set_c_id(c_id);
  h_row.set_c_d_id(c_d_id);
  h_row.set_c_w_id(c_w_id);
  h_row.set_d_id(d_id);
  h_row.set_w_id(w_id);
  h_row.set_data(w_row.name() + "    " + d_row.name());
  h_row.SerializeToString(&str);
  client.Put(HistoryRowKey(w_id, d_id, c_id), str, timeout); //TODO: should write to a unique key

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc
