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
#ifndef RW_BASE_TRANSACTION_H
#define RW_BASE_TRANSACTION_H

#include "store/benchmark/async/common/key_selector.h"

#include <vector>
#include <string>


namespace rwsync {

const std::string BENCHMARK_NAME = "rwsync";

class RWBaseTransaction {
 public:
  RWBaseTransaction(KeySelector *keySelector, int numOps, bool readOnly, std::mt19937 &rand);
  RWBaseTransaction() {};
  virtual ~RWBaseTransaction();

  inline const std::vector<int> getKeyIdxs() const {
    return keyIdxs;
  }
 protected:
  inline const std::string &GetKey(int i) const {
    return keySelector->GetKey(keyIdxs[i]);
  }

  inline const size_t GetNumOps() const { return numOps; }

  KeySelector *keySelector;
  size_t numOps;
  bool readOnly;
  std::vector<int> keyIdxs;
};

}

#endif /* RW_BASE_TRANSACTION_H */
