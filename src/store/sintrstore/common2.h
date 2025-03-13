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
#ifndef SINTR_COMMON2_H
#define SINTR_COMMON2_H

#include "lib/configuration.h"
#include "lib/keymanager.h"
#include "store/common/timestamp.h"
#include "store/sintrstore/query-proto.pb.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "lib/latency.h"
#include "store/sintrstore/verifier.h"
#include "lib/tcptransport.h"
#include "store/sintrstore/sql_interpreter.h"
#include "store/common/query_result/query_result_proto_wrapper.h"

namespace sintrstore {

// this function has to be declared here and not common.h because it creates circular header dependency there
bool ValidateTransactionTableWrite(const proto::CommittedProof &proof, const std::string *txnDigest, const Timestamp &timestamp, 
    const std::string &key, const std::string &value, const std::string &table_name, sql::QueryResultProtoWrapper *query_result,
    SQLTransformer *sql_interpreter,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier);

} // namespace sintrstore

#endif /* SINTR_COMMON2_H */