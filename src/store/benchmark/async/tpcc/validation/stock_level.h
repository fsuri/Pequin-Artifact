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
#ifndef VALDIATION_STOCK_LEVEL_H
#define VALDIATION_STOCK_LEVEL_H

#include "store/benchmark/async/tpcc/stock_level.h"
#include "store/benchmark/async/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/common/frontend/sync_client.h"

namespace tpcc {

class ValidationStockLevel : public ValidationTPCCTransaction, public StockLevel {
 public:
  // constructor with no randomness (all fields directly initialized)
  ValidationStockLevel(uint32_t timeout, uint32_t w_id, uint32_t d_id, uint8_t min_quantity);
  ValidationStockLevel(uint32_t timeout, const validation::proto::StockLevel &valStockLevelMsg);
  virtual ~ValidationStockLevel();
  virtual transaction_status_t Validate(::SyncClient &client);
};

} // namespace tpcc

#endif /* VALDIATION_STOCK_LEVEL_H */
