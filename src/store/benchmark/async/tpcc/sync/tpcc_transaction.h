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
#ifndef SYNC_TPCC_TRANSACTION_H
#define SYNC_TPCC_TRANSACTION_H

#include "store/benchmark/async/tpcc/tpcc-proto.pb.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"

namespace tpcc {


class SyncTPCCTransaction : public SyncTransaction {
 public:
  SyncTPCCTransaction(uint32_t timeout);
  virtual ~SyncTPCCTransaction();
  /*
    Right now just return a vector of tables that the transaction writes to,
    instead of the specific keys that the transaction writes to. 
    This is because reads could determine which keys to write to, 
    so it's impossible to know writeset keys ahead of executing the transaction
  */
  //TODO: generate heuristic function automatically (from application programmer input/from transaction)
  // then allow further modification...
  // right now return empty list...
  virtual std::vector<Tables> HeuristicFunction() {
    return {};
  }

};

}  // namespace tpcc

#endif /* SYNC_TPCC_TRANSACTION_H */
