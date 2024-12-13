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
#include "store/benchmark/async/tpcc/new_order.h"

#include <chrono>
#include <sstream>
#include <ctime>

#include "store/benchmark/async/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/async/tpcc/tpcc_utils.h"
#include "store/benchmark/async/tpcc/tpcc_common.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/common/common-proto.pb.h"


namespace tpcc {

NewOrder::NewOrder(uint32_t w_id, uint32_t C, uint32_t num_warehouses,
    std::mt19937 &gen) : w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  c_id = NURand(static_cast<uint32_t>(1023), static_cast<uint32_t>(1), static_cast<uint32_t>(3000), C, gen);
  ol_cnt = std::uniform_int_distribution<uint8_t>(5, 15)(gen);
  rbk = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
  all_local = true;
  for (uint8_t i = 0; i < ol_cnt; ++i) {
    if (rbk == 1 && i == ol_cnt - 1) {
      o_ol_i_ids.push_back(0);
    } else {
      o_ol_i_ids.push_back(NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1), static_cast<uint32_t>(100000), C, gen));
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

NewOrder::~NewOrder() {
}

void NewOrder::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState = TxnState();
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(TXN_NEW_ORDER));
  currTxnState.set_txn_name(txn_name);

  validation::proto::NewOrder curr_txn = validation::proto::NewOrder();
  curr_txn.set_w_id(w_id);
  curr_txn.set_d_id(d_id);
  curr_txn.set_c_id(c_id);
  curr_txn.set_ol_cnt(ol_cnt);
  curr_txn.set_rbk(rbk);
  *curr_txn.mutable_o_ol_i_ids() = {o_ol_i_ids.begin(), o_ol_i_ids.end()};
  *curr_txn.mutable_o_ol_supply_w_ids() = {o_ol_supply_w_ids.begin(), o_ol_supply_w_ids.end()};
  *curr_txn.mutable_o_ol_quantities() = {o_ol_quantities.begin(), o_ol_quantities.end()};
  curr_txn.set_o_entry_d(o_entry_d);
  curr_txn.set_all_local(all_local);
  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}

}
