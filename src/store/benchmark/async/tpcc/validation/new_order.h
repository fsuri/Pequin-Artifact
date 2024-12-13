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
#ifndef VALIDATION_NEW_ORDER_H
#define VALIDATION_NEW_ORDER_H

#include "store/benchmark/async/tpcc/new_order.h"
#include "store/benchmark/async/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/common/frontend/sync_client.h"

namespace tpcc {

class ValidationNewOrder : public ValidationTPCCTransaction, public NewOrder {
 public:
  // constructor with no randomness (all fields directly initialized)
  ValidationNewOrder(uint32_t timeout, uint32_t w_id, uint32_t d_id, 
    uint32_t c_id, uint8_t ol_cnt, uint8_t rbk, std::vector<uint32_t> o_ol_i_ids,
    std::vector<uint32_t> o_ol_supply_w_ids, std::vector<uint8_t> o_ol_quantities, 
    uint32_t o_entry_d, bool all_local);
  ValidationNewOrder(uint32_t timeout, validation::proto::NewOrder valNewOrderMsg);
  virtual ~ValidationNewOrder();
  virtual transaction_status_t Validate(::SyncClient &client);
};

} // namespace tpcc

#endif /* VALIDATION_NEW_ORDER_H */
