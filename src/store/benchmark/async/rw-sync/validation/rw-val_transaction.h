/***********************************************************************
 *
 * Copyright 2025 Austin Li <atl63@cornell.edu>
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
#ifndef RW_VAL_TRANSACTION_H
#define RW_VAL_TRANSACTION_H

#include "store/common/frontend/validation_transaction.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/sync_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/rw-sync/rw-base_transaction.h"
#include "store/benchmark/async/rw-sync/rw-validation-proto.pb.h"

#include <vector>


namespace rwsync {

class RWValTransaction : public ::ValidationTransaction, RWBaseTransaction {
 public:
  // constructor with no randomness
  RWValTransaction(uint32_t timeout, const validation::proto::RWSync &msg);
  virtual ~RWValTransaction();

  transaction_status_t Validate(::SyncClient &client);

 private:
  std::map<std::string, std::string> readValues;

};

}

#endif /* RW_VAL_TRANSACTION_H */
