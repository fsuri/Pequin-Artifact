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
#ifndef SINTR_TESTS_COMMON_H
#define SINTR_TESTS_COMMON_H

#include <gmock/gmock.h>

#include <map>
#include <set>
#include <sstream>

#include "store/common/common-proto.pb.h"
#include "store/common/timestamp.h"
#include "store/sintrstore/sintr-proto.pb.h"

namespace sintrstore {

void GenerateTestConfig(int g, int f, std::stringstream &ss);

void PopulateTransaction(const std::map<std::string, Timestamp> &readSet,
    const std::map<std::string, std::string> &writeSet, const Timestamp &ts,
    const std::set<int> &involvedGroups, proto::Transaction &txn);

void PopulateCommitProof(proto::CommittedProof &proof, int n);

} // namespace sintrstore

#endif /* SINTR_TESTS_COMMON_H */
